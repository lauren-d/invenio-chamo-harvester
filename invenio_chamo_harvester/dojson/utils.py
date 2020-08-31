

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
        print('COUCOU')
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
                print('subseries_title_value_part:', subseries_title_value_part)
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