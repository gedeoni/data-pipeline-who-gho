from __future__ import annotations
import logging
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse, quote
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Any, Iterator, Optional, Protocol
from etl.state import EtlStateRepository

log = logging.getLogger(__name__)

class Checkpointable(Protocol):
    def get_state(self, process_name: str) -> Optional[Any]: ...
    def set_checkpoint_state(self, process_name: str, state: dict[str, Any]): ...

class ODataClient:
    """A client for interacting with an OData API, with support for pagination and retries."""

    def __init__(
        self,
        base_url: str,
        state_repo: Optional[Checkpointable] = None,
        timeout: int = 30,
    ):
        self.base_url = base_url
        self.state_repo = state_repo
        self.timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _get_page(self, url: str) -> dict[str, Any]:
        """Fetches a single page of data from the given URL."""
        log.info(f"Fetching data from: {url}")
        try:
            with httpx.Client() as client:
                response = client.get(url, timeout=self.timeout, follow_redirects=True)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            log.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            log.error(f"An error occurred while requesting data: {e}")
            raise

    def get_all_data(
        self,
        entity_set: str,
        process_name: str,
        limit: Optional[int] = None,
        page_size: int = 100,
    ) -> Iterator[dict[str, Any]]:
        """
        Fetches all data from an entity set, handling pagination and resuming from a checkpoint.
        """
        next_link = self._get_initial_url(entity_set, process_name, page_size)
        records_fetched = 0
        while next_link:
            data = self._get_page(next_link)

            records = data.get("value", [])
            yield from records

            records_fetched += len(records)
            if limit and records_fetched >= limit:
                log.info(f"Development run limit of {limit} records reached.")
                break

            top, skip = self._get_paging_params(next_link, page_size)
            if len(records) < top:
                next_link = None
            else:
                next_link = self._set_paging_params(next_link, top, skip + top)
            if next_link and self.state_repo:
                self.state_repo.set_checkpoint_state(
                    process_name, {"next_link": next_link}
                )

        if self.state_repo:
            # Clear checkpoint on successful completion
            self.state_repo.set_checkpoint_state(process_name, {})


    def _get_initial_url(self, entity_set: str, process_name: str, page_size: int) -> str:
        """Determines the starting URL, using a checkpoint if available."""
        if self.state_repo:
            state = self.state_repo.get_state(process_name)
            if state and state.checkpoint_state and "next_link" in state.checkpoint_state:
                resumed_url = state.checkpoint_state["next_link"]
                log.info(f"Resuming extraction from checkpoint: {resumed_url}")
                return resumed_url

        initial_url = f"{self.base_url}/{entity_set}"
        initial_url = self._set_paging_params(initial_url, page_size, 0)
        log.info(f"Starting new extraction for {entity_set} from {initial_url}")
        return initial_url

    def _get_paging_params(self, url: str, default_top: int) -> tuple[int, int]:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        top = int(query.get("$top", [default_top])[0])
        skip = int(query.get("$skip", [0])[0])
        return top, skip

    def _set_paging_params(self, url: str, top: int, skip: int) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query["$top"] = [str(top)]
        query["$skip"] = [str(skip)]
        updated_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=updated_query))

def fetch_observations(
    client: ODataClient,
    indicator_codes: list[str],
    country_codes: list[str],
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Fetches observations for given indicators and countries."""
    observations_raw = []
    observations_fetched = 0

    for indicator_code in indicator_codes:
        if limit and observations_fetched >= limit:
            break

        for country_code in country_codes:
            if limit and observations_fetched >= limit:
                break

            query = urlencode(
                {
                    "$filter": f"SpatialDim eq '{country_code}'",
                    "$orderby": "TimeDim asc",
                },
                quote_via=quote,
            )
            entity_set = f"{indicator_code}?{query}"
            process_name = f"who_observations_{indicator_code}_{country_code}"

            remaining = limit - observations_fetched if limit else None

            batch = list(client.get_all_data(entity_set, process_name, limit=remaining))
            observations_raw.extend(batch)
            observations_fetched += len(batch)

    return observations_raw
