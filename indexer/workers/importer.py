"""
elasticsearch import pipeline worker
"""
import argparse
import logging
from datetime import datetime
from typing import Any, Optional, Union

from elastic_transport import ObjectApiResponse
from elasticsearch.exceptions import ConflictError, RequestError
from mcmetadata.urls import unique_url_hash

from indexer.app import run
from indexer.elastic import ElasticMixin
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

logger = logging.getLogger(__name__)

# Index name alias defined in the index_template.json
INDEX_NAME_ALIAS = "mc_search"


class ElasticsearchImporter(ElasticMixin, StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--no-output",
            action="store_false",
            dest="output_msgs",
            default=True,
            help="Disable output to archiver",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        self.output_msgs = self.args.output_msgs

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Process story and extract metadataurl
        """
        content_metadata = story.content_metadata().as_dict()
        if content_metadata:
            for key, value in content_metadata.items():
                if value is None or value == "":
                    logger.warning("Value for key: %s is not provided.", key)
                    continue

            keys_to_skip = ["is_homepage", "is_shortened", "parsed_date"]

            data: dict[str, Optional[Union[str, bool]]] = {
                k: v for k, v in content_metadata.items() if k not in keys_to_skip
            }

            # if publication date is None (from parser) or "None"(from archiver), fallback to rss_fetcher pub_date
            pub_date = data["publication_date"]
            if pub_date in [None, "None"]:
                rss_pub_date = story.rss_entry().pub_date
                if rss_pub_date:
                    pub_date = datetime.strptime(
                        rss_pub_date, "%a, %d %b %Y %H:%M:%S %z"
                    ).strftime("%Y-%m-%d")
                else:
                    pub_date = None

                data["publication_date"] = pub_date

                with story.content_metadata() as cmd:
                    cmd.publication_date = pub_date

            # Use parsed_date (from parser or an archive file) as indexed_date,
            # falling back to UTC now (for everything parsed/queued before
            # update applied to parser). API users use indexed_date to poll for
            # newly added stories, so a timestamp.  By default, ES stores times
            # with millisecond granularity.
            data["indexed_date"] = (
                content_metadata.get("parsed_date") or datetime.utcnow().isoformat()
            )
            response = self.import_story(data)
            if response and self.output_msgs:
                # pass story along to archiver (unless disabled)
                sender.send_story(story)

    def import_story(
        self,
        data: dict[str, Optional[Union[str, bool]]],
    ) -> Optional[ObjectApiResponse[Any]]:
        """
        Import a single story to Elasticsearch
        """
        response = None
        if data:
            url_hash = unique_url_hash(str(data.get("url")))
            # To move to Story index metadata
            data["indexed_date"] = datetime.utcnow().isoformat()
            target_index = INDEX_NAME_ALIAS
            try:
                response = self.elasticsearch_client().index(
                    id=url_hash, index=target_index, document=data
                )
            except ConflictError:
                self.incr("stories", labels=[("status", "dups")])
            except RequestError as e:
                self.incr("stories", labels=[("status", "reqerr")])
                raise QuarantineException(repr(e))
            except Exception:
                # Capture other exceptions here
                self.incr("stories", labels=[("status", "failed")])
                raise

            if response and response.get("result") == "created":
                logger.info(
                    f"Story has been successfully imported. to index {target_index}"
                )
                import_status_label = "success"
                self.incr("stories", labels=[("status", import_status_label)])

        return response


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")
