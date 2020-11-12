

"""Dojson utils."""

import re

from rero_ils.dojson.utils import ReroIlsMarc21Overdo, \
    TitlePartList, add_note, build_responsibility_data, error_print, \
    extract_subtitle_and_parallel_titles_from_field_245_b, get_field_items, \
    get_field_link_data, make_year, not_repetitive, \
    remove_trailing_punctuation


class CustomReroIlsMarc21Overdo(ReroIlsMarc21Overdo):

    def __init__(self, bases=None, entry_point_group=None):
        """Reroilsmarc21overdo init."""
        super(CustomReroIlsMarc21Overdo, self).__init__(
            bases=bases, entry_point_group=entry_point_group)
        self.extract_series_statement_subfield = {
            '440': {
                'series_title': 'a',
                'series_enumeration': 'v'
            },
            '490': {
                'series_title': 'a',
                'series_enumeration': 'v'
            },
            '773': {
                'series_title': 't',
                'series_enumeration': 'g'
            },
            '800': {
                'series_title': 't',
                'series_enumeration': 'v'
            },
            '830': {
                'series_title': 'a',
                'series_enumeration': 'v'
            }
        }

    def build_variant_title_data(self, string_set):
        """Build variant title data form fields 246.

        :param string_set: the marc field tag
        :type string_set: set
        :return: a list of variant_title object
        :rtype: list
        """
        variant_list = []
        fields_246 = self.get_fields(tag='246')
        for field_246 in fields_246:
            variant_data = {}
            subfield_246_a = ''
            subfields_246_a = self.get_subfields(field_246, 'a')
            if subfields_246_a:
                subfield_246_a = subfields_246_a[0]
            subfield_246_a_cleaned = remove_trailing_punctuation(
                subfield_246_a, ',.', ':;/-=')
            if subfield_246_a_cleaned not in string_set:
                # parse all subfields in order
                index = 1
                items = get_field_items(field_246['subfields'])
                tag_link, link = get_field_link_data(field_246)
                part_list = TitlePartList(
                    part_number_code='n',
                    part_name_code='p'
                )

                subfield_selection = {'a', 'b', 'n', 'p'}
                for blob_key, blob_value in items:
                    if blob_key in subfield_selection:
                        if blob_key == 'a':
                            subfield_a_parts = blob_value.split(':')
                            part_index = 0
                            for subfield_a_part in subfield_a_parts:
                                value_data = self. \
                                    build_value_with_alternate_graphic(
                                    '246', blob_key, subfield_a_part,
                                    index, link, ',.', ':;/-=')
                                if part_index == 0:
                                    variant_data['type'] = 'bf:VariantTitle'
                                    variant_data['mainTitle'] = value_data
                                else:
                                    variant_data['subtitle'] = value_data
                                part_index += 1
                        elif blob_key == 'b':
                            value_data = self. \
                                build_value_with_alternate_graphic(
                                '246', blob_key, blob_value,
                                index, link, ',.', ':;/-=')
                            variant_data['subtitle'] = value_data
                        elif blob_key in ['n', 'p']:
                            value_data = self. \
                                build_value_with_alternate_graphic(
                                '246', blob_key, blob_value,
                                index, link, ',.', ':;/-=')
                            if value_data:
                                part_list.update_part(
                                    value_data, blob_key, blob_value)
                    if blob_key != '__order__':
                        index += 1
                the_part_list = part_list.get_part_list()
                if the_part_list:
                    variant_data['part'] = the_part_list
                if variant_data:
                    variant_list.append(variant_data)
            else:
                pass
                # for showing the variant title skipped for debugging purpose
                # print('variant skipped', subfield_246_a_cleaned)
        return variant_list

    def extract_series_statement_from_440_field(self, value, data):
        """Extract the seriesStatement data from marc field data.

        This function automatically selects the subfield codes according field
        tag in the Marc21 or Unimarc format. The extracted data are:
        - seriesTitle
        - seriesEnumeration

        :param key: the field tag and indicators
        :param value: the subfields data
        :type value: object
        :param data: the object data on which the extracted data will be added
        :type data: object
        """
        # extract production_method from extent and physical_details
        tag_link, link = get_field_link_data(value)
        items = get_field_items(value)
        index = 1
        series = {}
        subseries = []
        subseries_titles = []
        subseries_title_value_part = []
        count = 0
        subcount = 0
        tag = '440'
        series_title_subfield_code = \
            self.extract_series_statement_subfield[tag]['series_title']
        series_enumeration_subfield_code = \
            self.extract_series_statement_subfield[tag]['series_enumeration']
        subfield_selection = \
            {series_title_subfield_code, series_enumeration_subfield_code}
        subseries_subfield_selection = {'n', 'p'}
        subfield_visited = ''
        for blob_key, blob_value in items:
            if blob_key in subfield_selection:
                subfield_visited += blob_key
                value_data = self.build_value_with_alternate_graphic(
                    tag, blob_key, blob_value, index, link, ',.', ':;/-=')
                if blob_key == series_title_subfield_code:
                    count += 1
                    if count == 1:
                        series['seriesTitle'] = value_data
                    # else:
                    #     subseries.append({'subseriesTitle': value_data})
                elif blob_key == series_enumeration_subfield_code:
                    if count == 1:
                        if 'seriesEnumeration' in series:
                            series['seriesEnumeration'] = \
                                join_alternate_graphic_data(
                                    alt_gr_1=series['seriesEnumeration'],
                                    alt_gr_2=value_data,
                                    join_str=', '
                                )
                        else:
                            series['seriesEnumeration'] = value_data
                    # elif count > 1:
                    #     if 'subseriesEnumeration' in subseries[count - 2]:
                    #         alt_gr_1 = \
                    #             subseries[count - 2]['subseriesEnumeration']
                    #         subseries[count - 2]['subseriesEnumeration'] = \
                    #             join_alternate_graphic_data(
                    #                 alt_gr_1=alt_gr_1,
                    #                 alt_gr_2=value_data,
                    #                 join_str=', '
                    #             )
                    #     else:
                    #         subseries[count - 2]['subseriesEnumeration'] = \
                    #             value_data

            if blob_key in subseries_subfield_selection:
                if blob_key == 'n':
                    subcount += 1
                    subseries_title_value_part.append(blob_value)
                elif blob_key == 'p':
                    if len(subseries_title_value_part) == 1:
                        subseries_title_value_part.append(blob_value)
                        subseries_titles.append(
                            {'value': ' '.join(subseries_title_value_part)}
                        )
                        subseries.append(
                            {'subseriesTitle': subseries_titles})
                        subseries_title_value_part.clear()
                        subseries_titles
                    elif count < 1:
                        subseries_titles.append(
                            {'value': blob_value}
                        )
                        subseries.append({'subseriesTitle': subseries_titles})
                        subseries_titles.clear()


            if blob_key != '__order__':
                index += 1

        error_msg = ''
        regexp = re.compile(r'^[^{}]'.format(series_title_subfield_code))
        if regexp.search(subfield_visited):
            error_msg = \
                'missing leading subfield ${code} in field {tag}'.format(
                    code=series_title_subfield_code,
                    tag=tag
                )
            error_print('ERROR BAD FIELD FORMAT:', self.bib_id, self.rero_id,
                        error_msg)
        else:
            if subseries:
                series['subseriesStatement'] = subseries
            series_statement = data.get('seriesStatement', [])
            if series:
                series_statement.append(series)
                data['seriesStatement'] = series_statement


def build_responsibility_data(responsibility_data):
    """Build the responsibility data form subfield $c of field 245.

    :param responsibility_data: list of responsibility_data
    :return: a list of responsibility
    :rtype: list
    """
    data_std = ''
    data_lang = ''
    lang = ''
    responsibilities = []
    for responsibility_value in responsibility_data:
        value = responsibility_value.get('value', '')
        lang = responsibility_value.get('language', '')
        if lang:
            data_lang = value
        else:
            data_std = value

    data_std_items = data_std.split(';')
    data_lang_items = []
    if data_lang:
        data_lang_items = data_lang.split(';')
    index = 0
    for data_std in data_std_items:
        out_data = []
        data_value = remove_trailing_punctuation(
                        data_std.lstrip(), ',.', ':;/-=')
        out_data.append({'value': data_value})
        if lang:
            try:
                data_lang_value = \
                    remove_trailing_punctuation(
                        data_lang_items[index].lstrip(), ',.', ':;/-=')
            except Exception as err:
                data_lang_value = '[missing data]'
            out_data.append({'value': data_lang_value, 'language': lang})
        index += 1
        responsibilities.append(out_data)
    return responsibilities


def clean_string_terminator(value):
    """Clean \u009c character in string."""
    return value.replace('\u009c', '')
