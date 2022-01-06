
def different_code_versions(code_version, lineage_obj):
    """
    Given the current version, see if it is different than found_version
    Note, if either version is dirty, we are forced to say they are different

    Typically we get the code_version from the pipe and the lineage object from the
    bundle.   We then see if the current code == the information in lineage object.

    Args:
        current_version (CodeVersion) :
        lineage_obj (LineageObject):

    Returns:

    """

    conf = common.DisdatConfig.instance()

    if conf.ignore_code_version:
        return False

    # If there were uncommitted changes, then we have to re-run, mark as different
    if code_version.dirty:
        return True

    if code_version.semver != lineage_obj.pb.code_semver:
        return True

    if code_version.hash != lineage_obj.pb.code_hash:
        return True

    ## Currently ignoring tstamp, branch, url
    ## CodeVersion = collections.namedtuple('CodeVersion', 'semver hash tstamp branch url dirty')

    return False
