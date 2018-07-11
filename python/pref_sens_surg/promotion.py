"""
### CODE OWNERS: Umang Gupta, Pierre Cornell

### OBJECTIVE:
  Tools to automate manual steps of code promotion

### DEVELOPER NOTES:
  When run as a script, should do code promotion process
"""
import logging
from pathlib import Path

import semantic_version

from indypy.nonstandard.ghapi_tools import repo
from indypy.nonstandard.ghapi_tools import conf
from indypy.nonstandard import promotion_tools

LOGGER = logging.getLogger(__name__)

_PATH_REL_RELEASE_NOTES = Path("docs") / "release-notes.md"
PATH_PROMOTION = Path(r"S:\PRM\Pipeline_Components\preference_sensitive_surgery")

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int: # pragma: no cover
    """Promotion process"""
    LOGGER.info("Beginning code promotion for product component")
    github_repository = repo.GithubRepository.from_parts("NYHealth", "preference-sensitive-surgery")
    version = semantic_version.Version(
        input("Please enter the semantic version number for this release (e.g. 1.2.3): "),
        partial=True,
        )
    promotion_branch = promotion_tools.prompt_for_promotion_branch()
    promotion_tools.validate_promotion_branch(promotion_branch, version)
    doc_info = promotion_tools.get_documentation_inputs(github_repository)
    release = promotion_tools.Release(github_repository, version, PATH_PROMOTION, doc_info)
    repository_clone = release.export_repo(branch=promotion_branch)
    release.make_release_json()
    if not version.prerelease:
        tag = release.make_tag(repository_clone)
        release.post_github_release(
            conf.get_github_oauth(prompt_if_no_file=True),
            tag,
            body=promotion_tools.get_release_notes(
                release.path_version / _PATH_REL_RELEASE_NOTES,
                version,
                ),
            )
    return 0


if __name__ == '__main__': # pragma: no cover
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    sys.exit(main())
