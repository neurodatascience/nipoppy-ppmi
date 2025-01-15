from logging import Logger

RE_IMAGE_ID = "(I[0-9]+)"  # regex


def get_all_descriptions(descriptions_dict, logger: Logger | None = None):

    to_log = []

    def _get_all_descriptions(descriptions_dict_or_list, descriptions, log_prefix):
        if isinstance(descriptions_dict_or_list, dict):
            for (
                key,
                descriptions_subdict_or_list,
            ) in descriptions_dict_or_list.items():
                to_log.append(f"{log_prefix}{key}:")
                descriptions = _get_all_descriptions(
                    descriptions_subdict_or_list, descriptions, f"\t{log_prefix}"
                )
        else:
            to_log.append(f" {len(descriptions_dict_or_list)}\n")
            descriptions.extend(descriptions_dict_or_list)
        return descriptions

    descriptions = _get_all_descriptions(descriptions_dict, [], "")
    if logger is not None:
        for line in "".join(to_log).split("\n"):
            if len(line) > 0:
                logger.debug(line)

    return descriptions
