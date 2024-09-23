from logging import Logger


def get_all_descriptions(descriptions_dict, logger: Logger | None = None):

    to_print = []

    def _get_all_descriptions(descriptions_dict_or_list, descriptions, print_prefix):
        if isinstance(descriptions_dict_or_list, dict):
            for (
                key,
                descriptions_subdict_or_list,
            ) in descriptions_dict_or_list.items():
                to_print.append(f"{print_prefix}{key}:")
                descriptions = _get_all_descriptions(
                    descriptions_subdict_or_list, descriptions, f"\t{print_prefix}"
                )
        else:
            to_print.append(f" {len(descriptions_dict_or_list)}\n")
            descriptions.extend(descriptions_dict_or_list)
        return descriptions

    descriptions = _get_all_descriptions(descriptions_dict, [], "")
    if logger is not None:
        for line in "".join(to_print).split("\n"):
            if len(line) > 0:
                logger.debug(line)

    return descriptions
