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
from dojson import Overdo, utils
from isbnlib import EAN13, clean, to_isbn13

marc21 = Overdo()


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


def get_mef_person_link(name, key, value):
    """Get mef person link."""
    # https://mef.test.rero.ch/api/mef/?q=rero.rero_pid:A012327677
    PROD_HOST = 'mef.rero.ch'
    DEV_HOST = 'mef.test.rero.ch'
    mef_url = None
    if name:
        person = remove_punctuation(name)
        url = "{mef}/?q={pers}&size=1".format(
            mef="https://{host}/api/mef".format(host=DEV_HOST),
            pers=person.lower(),
        )
        request = requests.get(url=url)
        if request.status_code == requests.codes.ok:
            data = request.json()
            hits = data.get('hits', {}).get('hits')
            if hits:
                mef_url = hits[0].get('links').get('self')
                mef_url = mef_url.replace(DEV_HOST, PROD_HOST)
        """
            else:
                print(
                    'ERROR: MEF person not found',
                    url,
                    key,
                    value,
                    file=sys.stderr
                )
        else:
            print(
                'ERROR: MEF request',
                url,
                request.status_code,
                file=sys.stderr
            )
        """
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
    language = self.get('language', [])
    lang_codes = list_of_langs(language)

    lang = {
        'value': 'und',
        'type': 'bf:Language'
    }

    # check len(value) to avoid getting char[35:38] if data is invalid
    if len(value) > 38:
        lang_value = value.strip()[35:38]
        if re.search(r'^[a-z]{3}$', lang_value):
            if lang_value not in lang_codes:
                if lang_value not in ['fre', 'ger', 'eng', 'ita',
                                      'spa', 'ara', 'chi', 'lat',
                                      'heb', 'jpn', 'por', 'rus']:
                    lang_value = 'und'
                lang['value'] = lang_value
    language.append(lang)
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
            identifier['acquisitionsTerms'] = subfield_c
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


@marc21.over('language', '^041..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_translatedFrom(self, key, value):
    """Get translatedFrom.

    languages: 041 [$a, repetitive]
    if language properties is already set form 008
    it will be replaced with those present in 041
    """
    language = self.get('language', [])
    lang_codes = list_of_langs(language)
    subfield_a = value.get('a')
    if subfield_a:
        for lang_value in utils.force_list(subfield_a):
            if language not in ['fre', 'ger', 'eng', 'ita', 'spa',
                                'ara', 'chi', 'lat', 'heb', 'jpn',
                                'por', 'rus']:
                lang_value = 'und'
            if lang_value not in lang_codes:
                lang = {
                    'value': lang_value,
                    'type': 'bf:Language'
                }
                language.append(lang)
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
            ref = get_mef_person_link(value.get('a'), key, value)
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


@marc21.over('publishers', '^260..')
@utils.for_each_value
@utils.ignore_value
def marc21_to_publishers_publicationDate(self, key, value):
    """Get publisher.

    publisher.name: 260 [$b repetitive] (without the , but keep the ;)
    publisher.place: 260 [$a repetitive] (without the : but keep the ;)
    publicationDate: 260 [$c repetitive] (but take only the first one)
    """
    publishers = self.get('publishers', [])
    publisher = {}
    if value.get('a'):
        places = []
        for a in utils.force_list(value.get('a')):
            if re.match(r'\[\s?(s|S)\.(d|D|n|N|l|e)\.?\]', a):
                continue
            places.append(remove_punctuation(a))
        if len(places):
            publisher['place'] = places

    if value.get('b'):
        names = []
        for b in utils.force_list(value.get('b')):
            if re.match(r'\[\s?(s|S)\.(d|D|n|N|l|e)\.?\]', b):
                continue
            names.append(remove_punctuation(b))
        if len(names):
            publisher['name'] = names

    if value.get('c'):
        dates = []
        for c in utils.force_list(value.get('c')):
            if re.match(r'\[\s?(s|S)\.(d|D|n|N|l|e)\.?\]', c):
                continue
            dates.append(remove_punctuation(c))

        c = ' '.join(utils.force_list(dates))

        # 4 digits
        m = re.match(r'.*?(\d{4}).*?', c)
        if m:
            date = m.group(1)
            self['publicationYear'] = int(date)
            if len(c) > 2 and c != str(self['publicationYear']):
                self['freeFormedPublicationDate'] = c
        else:
            # # create free form if different
            if len(c) > 2:
                self['freeFormedPublicationDate'] = c

    if publisher:
        publishers.append(publisher)

    return None


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
