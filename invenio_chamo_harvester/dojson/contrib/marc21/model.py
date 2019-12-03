# -*- coding: utf-8 -*-
#
# This file is part of RERO ILS.
# Copyright (C) 2017 RERO.
# Copyright (C) 2019 UCLouvain.
#
# RERO ILS is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# RERO ILS is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with RERO ILS; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, RERO does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""rero-ils MARC21 model definition."""

import re
import sys
from datetime import datetime

import click
import requests
from dojson import utils
from isbnlib import EAN13, clean, to_isbn13

from rero_ils.dojson.utils import ReroIlsMarc21Overdo, error_print, \
    get_field_items, get_field_link_data, make_year, not_repetitive, \
    remove_trailing_punctuation

marc21 = ReroIlsMarc21Overdo()


def list_of_langs(data):
    """Construct list of language codes from data."""
    lang_codes = []
    for lang_data in data:
        lang_codes.append(lang_data.get('value'))
    return lang_codes


def list_of_identifiers(data, type):
    """Construct list of identifiers for type from data."""
    identifiers = []
    for id_data in data:
        if id_data.get('type') == type:
            identifiers.append(id_data.get('value'))
    return identifiers


def remove_punctuation(data):
    """Remove punctuation from data."""
    try:
        if data[-1:] == ',':
            data = data[:-1]
        if data[-2:] == ' :':
            data = data[:-2]
        if data[-2:] == ' ;':
            data = data[:-2]
        if data[-2:] == ' /':
            data = data[:-2]
        if data[-2:] == ' -':
            data = data[:-2]
    except Exception:
        pass
    return data


def get_mef_person_link(id, key, value):
    """Get mef person link."""
    # https://mef.test.rero.ch/api/mef/?q=viaf_pid:67752559
    PROD_HOST = 'mef.rero.ch'
    DEV_HOST = 'mefdev.test.rero.ch'
    mef_url = None
    if id:
        url = "{mef}/?q=viaf_pid:{viaf_pid}&size=1".format(
            mef="https://{host}/api/mef".format(host=DEV_HOST),
            viaf_pid=id,
        )
        request = requests.get(url=url)
        if request.status_code == requests.codes.ok:
            data = request.json()
            hits = data.get('hits', {}).get('hits')
            if hits:
                mef_url = hits[0].get('links').get('self')
                mef_url = mef_url.replace(DEV_HOST, PROD_HOST)
    return mef_url


@marc21.over('type', 'leader')
def marc21_to_type(self, key, value):
    """
    Get document type.

    Books: LDR/6-7: am
    Journals: LDR/6-7: as
    Articles: LDR/6-7: aa + add field 773 (journal title)
    Scores: LDR/6: c|d
    Videos: LDR/6: g + 007/0: m|v
    Sounds: LDR/6: i|j
    """
    type_of_record = value[6]
    bibliographic_level = value[7]

    if type_of_record == 'a' and bibliographic_level == 'm':
        return 'book'
    if type_of_record == 'a' and bibliographic_level == 's':
        return 'journal'
    if type_of_record == 'a' and bibliographic_level == 'a':
        return 'article'
    if type_of_record in ['i', 'j']:
        return 'sound'
    if type_of_record == 'g':
        return 'video'
    if type_of_record in ['c', 'd']:
        return 'score'

    if bibliographic_level == 'm':
        return 'book'

    return 'score'


@marc21.over('identifiedBy', '^001')
@utils.ignore_value
def marc21_to_identifier_vtlsID(self, key, value):
    """Get identifier vtlsID.

    identifiers:vtlsID: 001
    """
    identifiers = self.get('identifiedBy', [])
    identifier = {
        'type': 'bf:Local',
        'source': 'VIRTUA',
        'value': value
    }
    identifiers.append(identifier)
    return identifiers


@marc21.over('language', '^008')
@utils.ignore_value
def marc21_to_language(self, key, value):
    """Get languages.
    languages: 008 and 041 [$a, repetitive]
    """
    lang_codes = []
    language = self.get('language', [])
    if marc21.lang_from_008 and not marc21.lang_from_008 =='   ':
        language.append({
            'value': marc21.lang_from_008,
            'type': 'bf:Language'
        })
        lang_codes.append(marc21.lang_from_008)
    for lang_value in marc21.langs_from_041_a:
        if lang_value not in lang_codes:
            language.append({
                'value': lang_value.strip(),
                'type': 'bf:Language'
            })
            lang_codes.append(lang_value)
    if not language :
    #     error_print('ERROR LANGUAGE:', marc21.bib_id, 'set to "und"')
        language = [{'value': 'und', 'type': 'bf:Language'}]
    return language or None


@marc21.over('identifiedBy', '^020..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_identifiedBy_from_field_020(self, key, value):
    """Get identifier from field 020."""
    def build_identifier_from(subfield_data, status=None):
        subfield_data = subfield_data.strip()
        identifier = {'value': subfield_data}
        subfield_c = value.get('c', '').strip()
        if subfield_c:
            identifier['acquisitionTerms'] = subfield_c
        if value.get('q'):  # $q is repetitive
            identifier['qualifier'] = \
                ', '.join(utils.force_list(value.get('q')))

        match = re.search(r'^(.+?)\s*\((.*)\)$', subfield_data)
        if match:
            # match.group(2): parentheses content
            identifier['qualifier'] = ', '.join(
                filter(
                    None,
                    [match.group(2), identifier.get('qualifier', '')]
                )
            )
            # value without parenthesis and parentheses content
            identifier['value'] = match.group(1)
        if status:
            identifier['status'] = status
        identifier['type'] = 'bf:Isbn'
        identifiedBy.append(identifier)

    identifiedBy = self.get('identifiedBy', [])
    isbns = list_of_identifiers(identifiedBy, 'bf:isbn')

    subfield_a = value.get('a')
    if subfield_a:
        for isbn_value in utils.force_list(subfield_a):
            if isbn_value not in isbns:
                build_identifier_from(isbn_value)

    subfield_z = value.get('z')
    if subfield_z:
        for isbn_value in utils.force_list(subfield_z):
            if isbn_value not in isbns:
                build_identifier_from(isbn_value,
                                      status='invalid or cancelled')

    return None


@marc21.over('identifiedBy', '^022..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_identifiedBy_from_field_022(self, key, value):
    """Get identifier from field 022."""
    status_for = {
        'm': 'cancelled',
        'y': 'invalid'
    }
    type_for = {
        'a': 'bf:Issn',
        'l': 'bf:IssnL',
        'm': 'bf:IssnL',
        'y': 'bf:Issn'
    }

    identifiedBy = self.get('identifiedBy', [])
    for subfield_code in ['a', 'l', 'm', 'y']:
        subfield_data = value.get(subfield_code, '').strip()
        if subfield_data:
            identifier = {}
            identifier['type'] = type_for[subfield_code]
            identifier['value'] = subfield_data
            if subfield_code in status_for:
                identifier['status'] = status_for[subfield_code]
            identifiedBy.append(identifier)
    return None


@marc21.over('title', '^245..')
@utils.ignore_value
def marc21_to_title(self, key, value):
    """Get title.

    title: 245 $a $b $c $h $n $p
    NB : ordered subfield like in virtua
     $a $b $n $p $c $h OR $a $n $p $b $c $h "
    """
    field_map = {
        'a': 'title',
        'b': 'remainder_of_title',
        'c': 'statement_of_responsibility',
        'h': 'medium',
        'n': 'number_of_part_section_of_a_work',
        'p': 'name_of_part_section_of_a_work',
    }
    titleParts = []
    for k, v in value.items():
        if k in ['a', 'b', 'n', 'p', 'c', 'h']:
            titleParts.append(' '.join(utils.force_list(v)))
    return ' '.join(titleParts)


@marc21.over('titlesProper', '^[17]30..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_titlesProper(self, key, value):
    """Test dojson marc21titlesProper.

    titleProper: 130 & 730 $a $p $g $s
    """
    titleParts = []
    for k, v in value.items():
        if k in ['a', 'p', 'g', 's']:
            titleParts.append(' '.join(utils.force_list(v)))
    return ' '.join(titleParts)


@marc21.over('authors', '[17][01][01]..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_author(self, key, value):
    """Get author.

    authors: loop:
    authors.name: 100$a [+ 100$b if it exists] or
        [700$a (+$b if it exists) repetitive] or
        [ 710$a repetitive (+$b if it exists, repetitive)]
    authors.date: 100 $d or 700 $d (facultatif)
    authors.qualifier: 100 $c or 700 $c (facultatif)
    authors.relator: 100 $c or 700 $c (facultatif)
    authors.type: if 100 or 700 then person, if 710 then organisation
    """
    if key[:3] in ['100', '700', '710', '711']:
        author = {}
        author['type'] = 'person'

        if value.get('a'):
            ref = get_mef_person_link(value.get('0'), key, value)
            if ref:
                author['$ref'] = ref

        # we do not have a $ref
        if not author.get('$ref'):
            author['name'] = remove_punctuation(value.get('a'))
            author_subs = utils.force_list(value.get('b'))
            if author_subs:
                for author_sub in author_subs:
                    author['name'] += ' ' + remove_punctuation(author_sub)
            if value.get('d'):
                author['date'] = remove_punctuation(value.get('d'))

            if key[:3] == '710':
                author['type'] = 'organisation'
            else:
                if value.get('c'):
                    author['qualifier'] = ''.join(
                        utils.force_list(value.get('c')))
        return author
    else:
        return None

@marc21.over('copyrightDate', '^260..')
@utils.ignore_value
def marc21_to_copyright_date(self, key, value):
    """Get Copyright Date."""
    copyright_dates = self.get('copyrightDate', [])
    copyrights_date = utils.force_list(value.get('c'))
    if copyrights_date:
        copyright_per_code = {
            'c': '©',
            'p': '℗',
            '©': '©',
            '℗': '℗'
        }
        for copyright_date in copyrights_date:
            match = re.search(r'^([©℗c])+\s*(\d{4}.*)', copyright_date)
            if match:
                
                copyright_date = ' '.join((
                    copyright_per_code[match.group(1)] ,
                    match.group(2)
                ))
                copyright_dates.append(copyright_date)
            # else:
            #     raise ValueError('Bad format of copyright date')
    return copyright_dates or None

@marc21.over('editionStatement', '^250..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_edition_statement(self, key, value):
    """Get edition statement data.
    editionDesignation: 250 [$a non repetitive] (without trailing /)
    responsibility: 250 [$b non repetitive]
    """
    key_per_code = {
        'a': 'editionDesignation',
        'b': 'responsibility'
    }

    def build_edition_data(code, label, index, link):
        data = [{'value': remove_trailing_punctuation(label)}]
        try:
            alt_gr = marc21.alternate_graphic['250'][link]
            subfield = \
                marc21.get_subfields(alt_gr['field'])[index]
            data.append({
                'value': remove_trailing_punctuation(subfield),
                'language': get_language_script(alt_gr['script'])
            })
        except Exception as err:
            pass
        return data

    tag_link, link = get_field_link_data(value)
    items = get_field_items(value)
    index = 1
    edition_data = {}
    subfield_selection = {'a', 'b'}
    for blob_key, blob_value in items:
        if blob_key in subfield_selection:
            subfield_selection.remove(blob_key)
            edition_data[key_per_code[blob_key]] = \
                build_edition_data(blob_key, blob_value, index, link)
        if blob_key != '__order__':
            index += 1
    return edition_data or None

@marc21.over('provisionActivity', '^260.[_0-3]')
@utils.for_each_value
@utils.ignore_value
def marc21_to_provisionActivity(self, key, value):
    """Get publisher data.
    publisher.name: 264 [$b repetitive] (without the , but keep the ;)
    publisher.place: 264 [$a repetitive] (without the : but keep the ;)
    publicationDate: 264 [$c repetitive] (but take only the first one)
    """
    def build_statement(field_value, ind2):

        def get_language_script(script):
            languages_scripts = {
                'arab': ('ara', 'per'),
                'cyrl': ('bel', 'chu', 'mac', 'rus', 'srp', 'ukr'),
                'grek': ('grc', 'gre'),
                'hani': ('chi', 'jpn'),
                'hebr': ('heb', 'lad', 'yid'),
                'jpan': ('jpn', ),
                'kore': ('kor', ),
                'zyyy': ('chi', )
            }
            if script in languages_scripts:
                languages = ([marc21.lang_from_008] +
                             marc21.langs_from_041_a +
                             marc21.langs_from_041_h)
                for lang in languages:
                    if lang in languages_scripts[script]:
                        return '-'.join([lang, script])
                error_print('WARNING LANGUAGE SCRIPTS:', marc21.bib_id,
                            script,  '008:', marc21.lang_from_008,
                            '041$a:', marc21.langs_from_041_a,
                            '041$h:', marc21.langs_from_041_h)
            return '-'.join(['und', script])

        def build_place_or_agent_data(code, label, index, link, add_country):
            type_per_code = {
                'a': 'bf:Place',
                'b': 'bf:Agent'
            }
            place_or_agent_data = {
                'type': type_per_code[code],
                'label': [{'value': remove_trailing_punctuation(label)}]
            }

            if add_country:
                if marc21.country:
                    place_or_agent_data['country'] = marc21.country
            try:
                alt_gr = marc21.alternate_graphic['260'][link]
                subfield = \
                    marc21.get_subfields(alt_gr['field'])[index]
                place_or_agent_data['label'].append({
                    'value': remove_trailing_punctuation(subfield),
                    'language': get_language_script(alt_gr['script'])
                })
            except Exception as err:
                pass
            return place_or_agent_data

        # function build_statement start here
        subfield_6 = field_value.get('6', '')
        tag_link = subfield_6.split('-')
        link = ''
        if len(tag_link) == 2:
            link = tag_link[1]

        statement = []
        if isinstance(field_value, utils.GroupableOrderedDict):
            items = field_value.iteritems(repeated=True)
        else:
            items = utils.iteritems(field_value)
        index = 1
        add_country = ind2 in ('_', '1')
        for blob_key, blob_value in items:
            if blob_key in ('a', 'b'):
                place_or_agent_data = build_place_or_agent_data(
                    blob_key, blob_value, index, link, add_country)
                if blob_key == 'a':
                    add_country = False
                statement.append(place_or_agent_data)
            if blob_key != '__order__':
                index += 1
        return statement

    # the function marc21_to_provisionActivity start here
    ind2 = key[4]
    type_per_ind2 = {
        '_': 'bf:Publication',
        '0': 'bf:Production',
        '1': 'bf:Publication',
        '2': 'bf:Distribution',
        '3': 'bf:Manufacture'
    }
    publication = {
        'type': type_per_ind2[ind2],
        'statement': [],
    }

    subfields_c = utils.force_list(value.get('c'))
    if subfields_c:
        subfield_c = subfields_c[0]
        publication['date'] = subfield_c
    click.echo(publication)
    if ind2 in ('_', '1'):
        publication['startDate'] = marc21.date1_from_008
        click.echo(publication['startDate'])
        if (marc21.date_type_from_008 != 'r' and
                marc21.date2_from_008 and
                marc21.date2_from_008 not in ('    ', '||||', '9999')):
            publication['endDate'] = marc21.date2_from_008
        if (marc21.date_type_from_008 == 'q' or
                marc21.date_type_from_008 == 'n'):
            publication['note'] = 'Date(s) incertaine(s) ou inconnue(s)'
    publication['statement'] = build_statement(value, ind2)
    return publication or None


@marc21.over('formats', '^300..')
@utils.ignore_value
def marc21_to_description(self, key, value):
    """Get extent, otherMaterialCharacteristics, formats.

    extent: 300$a (the first one if many)
    otherMaterialCharacteristics: 300$b (the first one if many)
    formats: 300 [$c repetitive]
    """
    if value.get('a'):
        if not self.get('extent', None):
            self['extent'] = remove_punctuation(
                utils.force_list(value.get('a'))[0]
            )
    if value.get('b'):
        if self.get('otherMaterialCharacteristics', []) == []:
            self['otherMaterialCharacteristics'] = remove_punctuation(
                utils.force_list(value.get('b'))[0]
            )
    if value.get('c'):
        formats = self.get('formats', None)
        if not formats:
            data = value.get('c')
            formats = list(utils.force_list(data))
        return formats
    else:
        return None


@marc21.over('series', '^440..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_series(self, key, value):
    """Get series.

    series.name: [490$a repetitive]
    series.number: [490$v repetitive]
    """
    series = {}
    name = value.get('a')
    if name:
        series['name'] = ', '.join(utils.force_list(name))
    number = value.get('v')
    if number:
        series['number'] = ', '.join(utils.force_list(number))
    return series


@marc21.over('abstracts', '^520..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_abstracts(self, key, value):
    """Get abstracts.

    abstract: [520$a repetitive]
    """
    if not value.get('a'):
        return None
    return ', '.join(utils.force_list(value.get('a')))


@marc21.over('notes', '^500..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_notes(self, key, value):
    """Get  notes.

    note: [500$a repetitive]
    """
    return utils.force_list(value.get('a'))[0]


@marc21.over('is_part_of', '^773..')
@utils.ignore_value
def marc21_to_is_part_of(self, key, value):
    """Get  is_part_of.

    is_part_of: [773$t repetitive]
    """
    if not self.get('is_part_of', None):
        return value.get('t')


@marc21.over('electronic_location', '^8564.')
@utils.for_each_value
@utils.ignore_value
def marc21_online_resources(self, key, value):
    """Get series.

    series.name: [490$a repetitive]
    series.number: [490$v repetitive]
    """
    return {'uri': value.get('u')}


@marc21.over('cover_art', '^9564.')
@utils.ignore_value
def marc21_cover_art(self, key, value):
    """Get cover art."""
    return value.get('u')


@marc21.over('subjects', '^6[0135][01].[06]')
@utils.for_each_value
@utils.ignore_value
def marc21_to_subjects(self, key, value):
    """Get subjects.

    subjects: 6xx [duplicates could exist between several vocabularies,
        if possible deduplicate]
    """
    if value.get('a'):
        subjects = self.get('subjects', [])
        subjects.append(', '.join(utils.force_list(value.get('a'))))
        self['subjects'] = list(set(subjects))
    return None
