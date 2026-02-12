import logging
import re
from datetime import datetime, UTC

import aiohttp
from packaging import version


logger = logging.getLogger(__name__)

_PATCHED = False


def _semver_key(tag_name: str):
    clean = re.sub(r'^v', '', tag_name)
    return version.parse(clean)


async def _fetch_tags(repo: str, limit: int = 30) -> list[dict]:
    url = f'https://api.github.com/repos/{repo}/tags?per_page={limit}'
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session, session.get(url) as response:
        if response.status != 200:
            return []
        data = await response.json()
        return data if isinstance(data, list) else []


def _latest_semver_tag(tags: list[dict]) -> str | None:
    parsed: list[tuple[version.Version, str]] = []
    for item in tags:
        tag_name = item.get('name')
        if not tag_name:
            continue
        try:
            parsed.append((_semver_key(tag_name), tag_name))
        except Exception:
            continue
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0], reverse=True)
    return parsed[0][1]


def apply_version_tags_fallback_patches() -> None:
    global _PATCHED
    if _PATCHED:
        return

    from app.services import version_service as version_service_module

    VersionInfo = version_service_module.VersionInfo
    VersionService = version_service_module.VersionService
    original_get_latest_stable_version = VersionService.get_latest_stable_version
    original_fetch_releases = VersionService._fetch_releases

    async def patched_get_latest_stable_version(self) -> str:
        latest = await original_get_latest_stable_version(self)
        if latest and latest != 'UNKNOW':
            return latest

        try:
            tags = await _fetch_tags(self.repo, limit=50)
            tag_name = _latest_semver_tag(tags)
            if tag_name:
                return tag_name
        except Exception as e:
            logger.warning('SpiderMan fallback tags failed in get_latest_stable_version: %s', e)

        return 'UNKNOW'

    async def patched_fetch_releases(self, force: bool = False):
        releases = await original_fetch_releases(self, force)
        if releases:
            return releases

        try:
            tags = await _fetch_tags(self.repo, limit=30)
            now_iso = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            fallback_releases = []
            for item in tags:
                tag_name = item.get('name')
                if not tag_name:
                    continue
                try:
                    _semver_key(tag_name)
                except Exception:
                    continue
                fallback_releases.append(
                    VersionInfo(
                        tag_name=tag_name,
                        published_at=now_iso,
                        name=tag_name,
                        body='Tag-only version (fallback, no GitHub release body).',
                        prerelease=False,
                    )
                )

            fallback_releases.sort(key=lambda r: r.version_obj, reverse=True)
            if fallback_releases:
                self._cache['releases'] = fallback_releases
                self._last_check = datetime.now()
                logger.info('SpiderMan fallback: loaded %d tags as releases for %s', len(fallback_releases), self.repo)
                return fallback_releases
        except Exception as e:
            logger.warning('SpiderMan fallback tags failed in _fetch_releases: %s', e)

        return releases

    VersionService.get_latest_stable_version = patched_get_latest_stable_version
    VersionService._fetch_releases = patched_fetch_releases
    _PATCHED = True
    logger.info('SpiderMan patch applied: version check fallback releases -> tags')

