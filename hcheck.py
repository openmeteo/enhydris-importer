#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Check whether the xls+hts files in the current directory follow the
specifications at http://hydroscope.gr/, Documents, Data entry
standardization.  We check whether the current directory contains a
single .xls or .odt file and whether the accompanying .hts files are
those described in the spreadsheet.
"""
import os
import logging
import re

import xlrd


class ExternalDataError(Exception):
    pass


class NumberOfSpreadsheetsError(ExternalDataError):

    def __str__(self):
        return \
            u"There must be exactly one xls or ods file in the directory;\n"\
            "but I see either none or more than one."


class UnsupportedSpreadsheetError(ExternalDataError):

    def __str__(self):
        return u"Oops, currently we only support .xls"


def entry_cmp(a, b):
    if a is None and b is None:
        return 0
    if a is None:
        return 1
    if b is None:
        return -1
    result = a['station_id'] - b['station_id']
    if result:
        return result
    result = a['variable_id'] - b['variable_id']
    if result:
        return result
    return a['step_id'] - b['step_id']


def get_integer_from_cell(sheet, row, column, accepted_values):
    result = sheet.cell_value(row, column)
    if sheet.cell_type(row, column) != xlrd.XL_CELL_NUMBER \
            or int(result) not in accepted_values:
        logging.error(
            u'Wrong cell value in {0}{1}; "{2}" not in accepted values'
            .format(chr(65 + column), row + 1, result))
        return -1
    return int(result)


def get_ids_from_spreadsheet(sheet):
    result = []
    for row in range(1, sheet.nrows):
        result.append(int(sheet.cell_value(row, 0)))
    return result


class ExternalDataChecker:

    def __init__(self):
        self.errors = False
        self.get_files()
        self.read_spreadsheet()
        self.read_filenames()

    def get_files(self):
        """
        Get self.spreadsheet (the spreadsheet found in the currect
        directory) and self.filenames (a list of the rest of the
        files). Raises exception if there isn't one and only one
        spreadsheet.
        """
        self.filenames = os.listdir('.')
        spreadsheets = []
        for filename in self.filenames:
            if filename[-4:] in ('.xls', '.ods'):
                spreadsheets.append(filename)
        if len(spreadsheets) != 1:
            raise NumberOfSpreadsheetsError()
        self.spreadsheet = spreadsheets[0]
        self.filenames.remove(self.spreadsheet)

    def read_spreadsheet(self):
        self.spreadsheet_entries = []
        if not self.spreadsheet.endswith('.xls'):
            raise UnsupportedSpreadsheetError()
        book = xlrd.open_workbook(self.spreadsheet)
        self.step_ids = get_ids_from_spreadsheet(book.sheet_by_name(u'Βήματα'))
        self.variable_ids = get_ids_from_spreadsheet(
            book.sheet_by_name(u'Μεταβλητές'))
        self.station_ids = get_ids_from_spreadsheet(
            book.sheet_by_name(u'Σταθμοί'))
        sheet = book.sheet_by_name(u'Χρονοσειρές')
        for row in range(2, sheet.nrows):
            if sheet.cell_type(row, 0) + sheet.cell_type(row, 4) \
                    + sheet.cell_type(row, 6) == 0:
                continue
            station_id = get_integer_from_cell(sheet, row, 0,
                                               self.station_ids)
            variable_id = get_integer_from_cell(sheet, row, 4,
                                                self.variable_ids)
            step_id = get_integer_from_cell(sheet, row, 6, self.step_ids)
            if station_id < 0 or variable_id < 0 or step_id < 0:
                continue
            self.spreadsheet_entries.append({'station_id': station_id,
                                             'variable_id': variable_id,
                                             'step_id': step_id,
                                             'row': row})

    def find_duplicate_spreadsheet_entries(self):
        self.spreadsheet_entries.sort(entry_cmp)
        entries = self.spreadsheet_entries[:]
        for i in range(len(entries) - 1):
            if not entry_cmp(entries[i], entries[i + 1]):
                self.errors = True
                logging.error(
                    'Duplicate record in spreadsheet rows {0} and {1}'
                    .format(entries[i]['row'] + 1,
                            entries[i + 1]['row'] + 1))
                self.spreadsheet_entries.remove(entries[i])

    def read_filenames(self):
        self.hts_entries = []
        self.pdf_entries = []
        for filename in self.filenames:
            if filename.endswith('.hts'):
                self.get_hts_entry(filename)
            elif filename.endswith('.pdf'):
                self.get_pdf_entry(filename)

    def get_hts_entry(self, filename):
        m = re.match(r'^(\d+)-(\d+)-(\d+)\.hts$', filename)
        if not m:
            self.errors = True
            logging.error(u'Filename "{0}" not understood'.format(filename))
            return
        self.hts_entries.append({'station_id': int(m.group(1)),
                                 'variable_id': int(m.group(2)),
                                 'step_id': int(m.group(3)),
                                 'filename': filename})

    def get_pdf_entry(self, filename):
        m = re.match(r'^(\d+)-(\d+)-(\d+).*\.pdf$', filename)
        if m:
            self.pdf_entries.append({'station_id': int(m.group(1)),
                                     'variable_id': int(m.group(2)),
                                     'step_id': int(m.group(3)),
                                     'filename': filename})
            return
        m = re.match(r'^(\d+).*\.pdf$', filename)
        if m:
            self.pdf_entries.append({'station_id': int(m.group(1)),
                                     'filename': filename})
            return
        self.errors = True
        logging.error(u'Filename "{0}" not understood'.format(filename))

    def cross_check_hts(self):
        self.spreadsheet_entries.sort(entry_cmp)
        self.hts_entries.sort(entry_cmp)
        i = j = 0
        while i < len(self.spreadsheet_entries) or j < len(self.hts_entries):
            s = self.spreadsheet_entries[i] \
                if i < len(self.spreadsheet_entries) else None
            h = self.hts_entries[j] if j < len(self.hts_entries) else None
            c = entry_cmp(s, h)
            if not c:
                i += 1
                j += 1
                continue
            if c > 0:
                self.errors = True
                logging.error(
                    u'File {0} is not registered in the spreadsheet'
                    .format(h['filename']))
                j += 1
                continue
            if c < 0:
                self.errors = True
                logging.error(
                    u'File {0}-{1}-{2}.hts does not exist '
                    '(spreadsheet row {3})'
                    .format(s['station_id'], s['variable_id'], s['step_id'],
                            s['row'] + 1))
                i += 1

    def check(self):
        self.find_duplicate_spreadsheet_entries()
        self.cross_check_hts()
        if self.errors:
            raise ExternalDataError(
                "One or more errors occurred while checking the files")


if __name__ == '__main__':
    c = ExternalDataChecker()
    c.check()
