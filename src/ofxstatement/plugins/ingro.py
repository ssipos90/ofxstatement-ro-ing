import csv
import re
import hashlib

from decimal import Decimal, Decimal as D
from typing import Dict, Optional, Any, Iterable, List, TextIO, TypeVar, Generic
from ofxstatement import statement
from ofxstatement.plugin import Plugin
from ofxstatement.parser import CsvStatementParser,StatementLine

import locale

from pprint import pformat, pprint


class IngRoPlugin(Plugin):
    """ING Romania Plugin
    """

    def get_parser(self, filename):
        f = open(filename, 'r', encoding=self.settings.get("charset", "ISO-8859-2"))
        parser = IngRoParser(f)
        return parser


class IngRoParser(CsvStatementParser):

    # 0-based csv column mapping to StatementLine field
    mappings = {
        'date': 0,
        'memo': 1,
        'amount': 2
    }

    refnum_pattern = re.compile('(Referinta|Autorizare): (\\d+)')

    date_format = "%d %B %Y"

    currentRecord = {
        'date': '',
        'details': '',
        'amount': 0.0,
        'type': None,
        'refnum': None
    }

    def parse(self):
        locale.setlocale(locale.LC_ALL, 'ro_RO.UTF-8')
        stmt = super(IngRoParser, self).parse()
        statement.recalculate_balance(stmt)
        return stmt

    def parse_record(self, line: List[str]) -> Optional[StatementLine]:
        #print("\n[[[[ parsing record: " + pformat(line))
        (date, reserved1, reserved2, details, reserved3, debit_amount, credit_amount) = line

        # Skip header
        if date == 'Data':
            return None

        # Skip other stuff
        if date.startswith('Titular cont'):
            return None

        #print(">>>>> date is: " + date)
        #print(">>>>> recorded date is: " + self.currentRecord['date'])

        debit_amount = debit_amount.replace(".", "").replace(",", ".") if debit_amount != '' else '0.0'
        credit_amount = credit_amount.replace(".", "").replace(",", ".") if credit_amount != '' else '0.0'

        if debit_amount != '0.0':
            statement_amount = debit_amount
            statement_type = 'DEBIT'
        elif credit_amount != '0.0':
            statement_amount = credit_amount
            statement_type = 'CREDIT'
        else:
            print("none statement?", line)
            statement_amount = '0.0'
            statement_type = 'NONE'

        # Here we could commit the previous transaction because:
        # 1. We either start a new transaction (date field is valid)
        # 2. We reached the end of the file (reserved1 field is valid, and date is None)
        # However, we might not have a previous transaction (this is the first), so check if there is
        # anything to commit at this point.

        if date != '':
            statement_line = None
            if self.currentRecord['date'] != '':
                #print("----> Output currentRecord" + pformat(self.currentRecord))

                statement_line = StatementLine()
                statement_line.amount = self.currentRecord['amount']
                statement_line.date = self.currentRecord['date']
                statement_line.memo = self.currentRecord['details']
                statement_line.trntype = self.currentRecord['type']
                statement_line.refnum = self.currentRecord['refnum'] or hashlib.md5(self.currentRecord['details'].encode()).hexdigest()

            # print("##### We started a new record with date: " + date)
            self.currentRecord['date'] = self.parse_datetime(date)
            self.currentRecord['details'] = details
            self.currentRecord['amount'] = D(statement_amount)
            self.currentRecord['type'] = statement_type
            self.currentRecord['refnum'] = None

            return statement_line

        if reserved1 != '':
            # We are at the end of the file where the bank/account manager signatures
            # are found in the reserved fields. This means that there's no current record to
            # commit.
            # print("----- We are at the end of the file")
            statement_line = None
            if self.currentRecord['date'] != '':
                # print("----> Output currentRecord" + pformat(self.currentRecord))
                statement_line = StatementLine()
                statement_line.amount = self.currentRecord['amount']
                statement_line.date = self.currentRecord['date']
                statement_line.memo = self.currentRecord['details']
                statement_line.trntype = self.currentRecord['type']
                statement_line.refnum = self.currentRecord['refnum'] or hashlib.md5(self.currentRecord['details'].encode()).hexdigest()

            # This is a record from the end of the file, where we do not have any record data.
            self.currentRecord['date'] = ''
            self.currentRecord['details'] = ''
            self.currentRecord['amount'] = D('0.0')
            self.currentRecord['type'] = None
            self.currentRecord['refnum'] = None
            return statement_line

        if date == '':
            # This line contains extra details for the current transaction
            # print("***** Adding details: " + details)
            self.currentRecord['details'] = self.currentRecord['details'] + " " + details
            m = self.refnum_pattern.search(details)
            #print('** Matching: ', details, m)
            if m != None:
                self.currentRecord['refnum'] = m.group(2)

            return None
