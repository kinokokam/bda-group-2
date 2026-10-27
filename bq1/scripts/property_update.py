"""Build a Managed Flink environment-property update from the current groups.

Reads the application's existing property group descriptions on stdin and
writes an `EnvironmentPropertyUpdates` document on stdout. An update replaces
every property group, so the existing groups are merged rather than rebuilt:
omitting one drops it, and dropping the run-options group makes the service
reject the call.

Usage: `property_update.py key=value [key=value ...]` with the groups on stdin.
"""

import json
import sys

TARGET_GROUP = "BQ1"


def parse_assignments(arguments: list[str]) -> dict[str, str]:
    """Split `key=value` arguments into a mapping.

    Parameters
    ----------
    arguments : list[str]
        Assignments as given on the command line.

    Returns
    -------
    dict[str, str]
        Parsed keys and values.

    Raises
    ------
    ValueError
        If an argument has no `=`.
    """
    assignments = {}
    for argument in arguments:
        key, separator, value = argument.partition("=")
        if not separator:
            raise ValueError(f"expected key=value, got {argument!r}")
        assignments[key] = value
    return assignments


def build_update(groups: list[dict], assignments: dict[str, str]) -> dict:
    """Merge the given keys into the target group and wrap the result.

    Parameters
    ----------
    groups : list[dict]
        Property group descriptions as returned by describe-application.
    assignments : dict[str, str]
        Keys to set on the target group.

    Returns
    -------
    dict
        An application-configuration-update document.

    Raises
    ------
    KeyError
        If the target property group is absent.
    """
    updated = []
    has_target = False
    for group in groups:
        properties = dict(group["PropertyMap"])
        if group["PropertyGroupId"] == TARGET_GROUP:
            has_target = True
            properties.update(assignments)
        updated.append({"PropertyGroupId": group["PropertyGroupId"], "PropertyMap": properties})

    if not has_target:
        raise KeyError(f"property group {TARGET_GROUP} missing from the application")

    return {"EnvironmentPropertyUpdates": {"PropertyGroups": updated}}


def self_check() -> None:
    """Verify merging preserves other groups and overwrites target keys.

    Returns
    -------
    None
    """
    groups = [
        {"PropertyGroupId": "BQ1", "PropertyMap": {"run.id": "old", "bucket": "aml-bq1"}},
        {"PropertyGroupId": "kinesis.analytics.flink.run.options", "PropertyMap": {"python": "main.py"}},
    ]
    result = build_update(groups, {"run.id": "new", "gold.from": "2022-09-08 00:00:00"})
    written = {
        group["PropertyGroupId"]: group["PropertyMap"]
        for group in result["EnvironmentPropertyUpdates"]["PropertyGroups"]
    }
    assert written["BQ1"]["run.id"] == "new"
    assert written["BQ1"]["bucket"] == "aml-bq1"
    assert written["BQ1"]["gold.from"] == "2022-09-08 00:00:00"
    assert written["kinesis.analytics.flink.run.options"]["python"] == "main.py"
    assert parse_assignments(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}
    try:
        parse_assignments(["bare"])
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for an argument without '='")


def main() -> None:
    """Read groups from stdin and write the update document to stdout.

    Returns
    -------
    None
    """
    if sys.argv[1:2] == ["--self-check"]:
        self_check()
        print("ok")
        return
    assignments = parse_assignments(sys.argv[1:])
    groups = json.load(sys.stdin)
    json.dump(build_update(groups, assignments), sys.stdout)


if __name__ == "__main__":
    main()
