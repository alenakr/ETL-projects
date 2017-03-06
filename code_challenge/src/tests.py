import unittest
import os
import ast

from ltv import TopXSimpleLTVCustomers, Ingest, Input, Output, ConvertDate

test_D1 = {'IMAGE': {'d8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d16f': ['2017-01-26:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d9f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None]},
          'ORDER': {'68d84e5d1a61': ['2017-01-10:13:55:55.555Z', '96f55c7d8f49', '1.34 USD', None],
                    '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None],
                    '68d84e5d1a63': ['2017-01-08:12:55:55.555Z', '96f55c7d8f48', '2.34 USD', None],
                    '68d84e5d1a51': ['2017-01-28:13:55:55.555Z', '96f55c7d8f42', '36.34 USD', None],
                    '68d84e5d1a57': ['2017-01-29:13:55:55.555Z', '96f55c7d8f45', '4.09 UwSD', None],
                    '68d84e5d1a50': ['2017-01-26:12:55:55.555Z', '96f55c7d8f42', '1.34 USD', None],
                    '68d84e5d1a48': ['2017-01-08:12:55:55.555Z', '96f55c7d8f46', '24.04 USD', None],
                    '68d84e5d1a46': ['2017-01-08:12:55:55.555Z', '96f55c7d8f44', '0.04 USD', None],
                    '68d84e5d1a56': ['2017-01-26:12:55:55.555Z', '96f55c7d8f44', '0 USD', None],
                    '68d84e5d1a58': ['2017-01-27:12:55:55.555Z', '96f55c7d8f46', '6.04 USD', None],
                    '68d84e5d1a55': ['2017-01-29:13:55:55.555Z', '96f55c7d8f43', '54.7 USD', None],
                    '68d84e5d1a66': ['2017-01-08:12:55:55.555Z', '96f55c7d8f54', '4 USD', None],
                    '68d84e5d1a67': ['2017-01-10:13:55:55.555Z', '96f55c7d8f55', '120 USD', None],
                    '68d84e5d1a54': ['2017-01-26:12:55:55.555Z', '96f55c7d8f43', '0.14 USD', None],
                    '68d84e5d1a41': ['2017-01-10:13:55:55.555Z', '96f55c7d8f42', '16.34 USD', None],
                    '68d84e5d1a49': ['2017-01-10:13:55:55.555Z', '96f55c7d8f47', '12 USD', None],
                    '68d84e5d1a68': ['2017-01-08:12:55:55.555Z', '96f55c7d8f56', '24.04 USD', None],
                    '68d84e5d1a65': ['2017-01-10:13:55:55.555Z', '96f55c7d8f50', '9 USD', None],
                    '68d84e5d1a69': ['2017-01-10:13:55:55.555Z', '96f55c7d8f57', '12 USD', None],
                    '68d84e5d1a64': ['2017-01-08:12:55:55.555Z', '96f55c7d8f50', '39.04 USD', None],
                    '68d84e5d1a43': ['2017-01-08:12:55:55.555Z', '96f55c7d8f42', '12.34 USD', None],
                    '68d84e5d1a44': ['2017-01-08:12:55:55.555Z', '96f55c7d8f43', '39.04 USD', None],
                    '68d84e5d1a47': ['2017-01-10:13:55:55.555Z', '96f55c7d8f45', '120 USD', None],
                    '68d84e5d1a59': ['2017-01-28:13:55:55.555Z', '96f55c7d8f47', '80 USdD', None]},
          'CUSTOMER': {'96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None],
                       '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None]},
          'SITE_VISIT': {'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None],
                         'ac05e8155028f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f56', {'some key': 'some value'}, None],
                         'ac05e8155012f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155010f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155029f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f57', {'some key': 'some value'}, None],
                         'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None],
                         'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None],
                         'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155013f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e8155023f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f55', {'some key': 'some value'}, None],
                         'ac05e8155022f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f54', {'some key': 'some value'}, None],
                         'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None],}}

output_test1 = "96f55c7d8f43 : 82014.4, 96f55c7d8f45 : 62400.0, 96f55c7d8f55 : 62400.0, 96f55c7d8f42 : 54100.8, 96f55c7d8f47 : 47840.0, 96f55c7d8f50 : 24980.8, 96f55c7d8f56 : 12500.8, 96f55c7d8f57 : 6240.0, 96f55c7d8f46 : 3140.8, 96f55c7d8f54 : 2080.0, "

test_D2 = {'IMAGE': {'d8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None]},
          'ORDER': {'68d84e5d1a61': ['2017-01-10:13:55:55.555Z', '96f55c7d8f49', '1.34 USD', None],
                    '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None],
                    '68d84e5d1a63': ['2017-01-08:12:55:55.555Z', '96f55c7d8f48', '2.34 USD', None],
                    '68d84e5d1a57': ['2017-01-29:13:55:55.555Z', '96f55c7d8f45', '4.09 UwSD', None],
                    '68d84e5d1a48': ['2017-01-08:12:55:55.555Z', '96f55c7d8f46', '24.04 USD', None],
                    '68d84e5d1a46': ['2017-01-08:12:55:55.555Z', '96f55c7d8f44', '0.04 USD', None],
                    '68d84e5d1a56': ['2017-01-26:12:55:55.555Z', '96f55c7d8f44', '0 USD', None],
                    '68d84e5d1a58': ['2017-01-27:12:55:55.555Z', '96f55c7d8f46', '6.04 USD', None],
                    '68d84e5d1a55': ['2017-01-29:13:55:55.555Z', '96f55c7d8f43', '54.7 USD', None],
                    '68d84e5d1a66': ['2017-01-08:12:55:55.555Z', '96f55c7d8f54', '4 USD', None],
                    '68d84e5d1a67': ['2017-01-10:13:55:55.555Z', '96f55c7d8f55', '120 USD', None],
                    '68d84e5d1a54': ['2017-01-26:12:55:55.555Z', '96f55c7d8f43', '0.14 USD', None],
                    '68d84e5d1a49': ['2017-01-10:13:55:55.555Z', '96f55c7d8f47', '12 USD', None],
                    '68d84e5d1a68': ['2017-01-08:12:55:55.555Z', '96f55c7d8f56', '24.04 USD', None],
                    '68d84e5d1a65': ['2017-01-10:13:55:55.555Z', '96f55c7d8f50', '9 USD', None],
                    '68d84e5d1a69': ['2017-01-10:13:55:55.555Z', '96f55c7d8f57', '12 USD', None],
                    '68d84e5d1a64': ['2017-01-08:12:55:55.555Z', '96f55c7d8f50', '39.04 USD', None],
                    '68d84e5d1a44': ['2017-01-08:12:55:55.555Z', '96f55c7d8f43', '39.04 USD', None],
                    '68d84e5d1a47': ['2017-01-10:13:55:55.555Z', '96f55c7d8f45', '120 USD', None],
                    '68d84e5d1a59': ['2017-01-28:13:55:55.555Z', '96f55c7d8f47', '80 USdD', None]},
          'CUSTOMER': {'96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None],
                       '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f56': ['jmn2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],},
          'SITE_VISIT': {'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None],
                         'ac05e8155028f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f56', {'some key': 'some value'}, None],
                         'ac05e8155012f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155010f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155029f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f57', {'some key': 'some value'}, None],
                         'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None],
                         'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None],
                         'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155013f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e8155023f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f55', {'some key': 'some value'}, None],
                         'ac05e8155022f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f54', {'some key': 'some value'}, None],
                         'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None],}}

output_test2 = "96f55c7d8f43 : 82014.4, 96f55c7d8f45 : 62400.0, 96f55c7d8f55 : 62400.0, 96f55c7d8f47 : 47840.0, 96f55c7d8f50 : 24980.8, 96f55c7d8f56 : 12500.8, 96f55c7d8f57 : 6240.0, 96f55c7d8f46 : 3140.8, 96f55c7d8f54 : 2080.0, 96f55c7d8f48 : 1216.8, "

test_D3 = {'IMAGE': {'d8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d16f': ['2017-01-26:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d9f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None]},
          'ORDER': {'68d84e5d1a61': ['2017-01-10:13:55:55.555Z', '96f55c7d8f49', '1.34 USD', None],
                    '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None],
                    '68d84e5d1a63': ['2017-01-08:12:55:55.555Z', '96f55c7d8f48', '2.34 USD', None],
                    '68d84e5d1a51': ['2017-01-28:13:55:55.555Z', '96f55c7d8f42', '36.34 USD', None],
                    '68d84e5d1a50': ['2017-01-26:12:55:55.555Z', '96f55c7d8f42', '1.34 USD', None],
                    '68d84e5d1a48': ['2017-01-08:12:55:55.555Z', '96f55c7d8f46', '24.04 USD', None],
                    '68d84e5d1a46': ['2017-01-08:12:55:55.555Z', '96f55c7d8f44', '0.04 USD', None],
                    '68d84e5d1a56': ['2017-01-26:12:55:55.555Z', '96f55c7d8f44', '0 USD', None],
                    '68d84e5d1a58': ['2017-01-27:12:55:55.555Z', '96f55c7d8f46', '6.04 USD', None],
                    '68d84e5d1a64': ['2017-01-08:12:55:55.555Z', '96f55c7d8f50', '39.04 USD', None],
                    '68d84e5d1a43': ['2017-01-08:12:55:55.555Z', '96f55c7d8f42', '12.34 USD', None],
                    '68d84e5d1a44': ['2017-01-08:12:55:55.555Z', '96f55c7d8f43', '39.04 USD', None],
                    '68d84e5d1a47': ['2017-01-10:13:55:55.555Z', '96f55c7d8f45', '120 USD', None],
                    '68d84e5d1a59': ['2017-01-28:13:55:55.555Z', '96f55c7d8f47', '80 USdD', None]},
          'CUSTOMER': {'96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None],
                       '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None]},
          'SITE_VISIT': {'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None],
                         'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None],
                         'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None],
                         'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None],}}

output_test3 = "96f55c7d8f45 : 62400.0, 96f55c7d8f47 : 41600.0, 96f55c7d8f42 : 26010.4, 96f55c7d8f43 : 24980.8, 96f55c7d8f50 : 20300.8, 96f55c7d8f46 : 3140.8, 96f55c7d8f48 : 1216.8, 96f55c7d8f49 : 696.8, 96f55c7d8f44 : 20.8, "

test_D4 = {'IMAGE': {'d8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d16f': ['2017-01-26:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d9f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None],
                    'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None]},
          'ORDER': {'68d84e5d1a61': ['2017-01-10:13:55:55.555Z', '96f55c7d8f49', '1.34 USD', None],
                    '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None],
                    '68d84e5d1a57': ['2017-01-29:13:55:55.555Z', '96f55c7d8f45', '4.09 UwSD', None],
                    '68d84e5d1a50': ['2017-01-26:12:55:55.555Z', '96f55c7d8f42', '1.34 USD', None],
                    '68d84e5d1a58': ['2017-01-27:12:55:55.555Z', '96f55c7d8f46', '6.04 USD', None],
                    '68d84e5d1a55': ['2017-01-29:13:55:55.555Z', '96f55c7d8f43', '54.7 USD', None],
                    '68d84e5d1a66': ['2017-01-08:12:55:55.555Z', '96f55c7d8f54', '4 USD', None],
                    '68d84e5d1a67': ['2017-01-10:13:55:55.555Z', '96f55c7d8f55', '120 USD', None],
                    '68d84e5d1a54': ['2017-01-26:12:55:55.555Z', '96f55c7d8f43', '0.14 USD', None],
                    '68d84e5d1a65': ['2017-01-10:13:55:55.555Z', '96f55c7d8f50', '9 USD', None],
                    '68d84e5d1a69': ['2017-01-10:13:55:55.555Z', '96f55c7d8f57', '12 USD', None],
                    '68d84e5d1a59': ['2017-01-28:13:55:55.555Z', '96f55c7d8f47', '80 USdD', None]},
          'CUSTOMER': {'96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None],
                       '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None],
                       '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None],
                       '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None]},
          'SITE_VISIT': {'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None],
                         'ac05e8155028f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f56', {'some key': 'some value'}, None],
                         'ac05e8155012f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155010f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155029f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f57', {'some key': 'some value'}, None],
                         'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None],
                         'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None],
                         'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None],
                         'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None],
                         'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155013f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None],
                         'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None],
                         'ac05e8155023f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f55', {'some key': 'some value'}, None],
                         'ac05e8155022f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f54', {'some key': 'some value'}, None],
                         'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None],
                         'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None],}}

output_test4 = "96f55c7d8f55 : 62400.0, 96f55c7d8f43 : 61713.6, 96f55c7d8f47 : 41600.0, 96f55c7d8f57 : 6240.0, 96f55c7d8f50 : 4680.0, 96f55c7d8f46 : 3140.8, 96f55c7d8f54 : 2080.0, 96f55c7d8f42 : 1393.6, 96f55c7d8f49 : 696.8, 96f55c7d8f56 : 0.0, "

test_Ingest_D1 = {'CUSTOMER':{},'SITE_VISIT':{},'IMAGE':{},'ORDER':{}}
test_Ingest_D1_result = {'IMAGE': {'d8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None], 'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None], 'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None], 'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None], 'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None], 'd8ede43b1d9f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None], 'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None], 'd8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None], 'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None], 'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None], 'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None], 'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None], 'd8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None], 'd8ede43b1d16f': ['2017-01-26:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None], 'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None]}, 'ORDER': {'68d84e5d1a69': ['2017-01-10:13:55:55.555Z', '96f55c7d8f57', '12 USD', None], '68d84e5d1a66': ['2017-01-08:12:55:55.555Z', '96f55c7d8f54', '4 USD', None], '68d84e5d1a44': ['2017-01-08:12:55:55.555Z', '96f55c7d8f43', '39.04 USD', None], '68d84e5d1a41': ['2017-01-10:13:55:55.555Z', '96f55c7d8f42', '16.34 USD', None], '68d84e5d1a55': ['2017-01-29:13:55:55.555Z', '96f55c7d8f43', '54.7 USD', None], '68d84e5d1a43': ['2017-01-08:12:55:55.555Z', '96f55c7d8f42', '12.34 USD', None], '68d84e5d1a64': ['2017-01-08:12:55:55.555Z', '96f55c7d8f50', '39.04 USD', None], '68d84e5d1a61': ['2017-01-10:13:55:55.555Z', '96f55c7d8f49', '1.34 USD', None], '68d84e5d1a67': ['2017-01-10:13:55:55.555Z', '96f55c7d8f55', '120 USD', None], '68d84e5d1a49': ['2017-01-10:13:55:55.555Z', '96f55c7d8f47', '12 USD', None], '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None], '68d84e5d1a63': ['2017-01-08:12:55:55.555Z', '96f55c7d8f48', '2.34 USD', None], '68d84e5d1a48': ['2017-01-08:12:55:55.555Z', '96f55c7d8f46', '24.04 USD', None], '68d84e5d1a46': ['2017-01-08:12:55:55.555Z', '96f55c7d8f44', '0.04 USD', None], '68d84e5d1a59': ['2017-01-28:13:55:55.555Z', '96f55c7d8f47', '80 USdD', None], '68d84e5d1a50': ['2017-01-26:12:55:55.555Z', '96f55c7d8f42', '1.34 USD', None], '68d84e5d1a58': ['2017-01-27:12:55:55.555Z', '96f55c7d8f46', '6.04 USD', None], '68d84e5d1a56': ['2017-01-26:12:55:55.555Z', '96f55c7d8f44', '0 USD', None], '68d84e5d1a65': ['2017-01-10:13:55:55.555Z', '96f55c7d8f50', '9 USD', None], '68d84e5d1a68': ['2017-01-08:12:55:55.555Z', '96f55c7d8f56', '24.04 USD', None], '68d84e5d1a54': ['2017-01-26:12:55:55.555Z', '96f55c7d8f43', '0.14 USD', None], '68d84e5d1a47': ['2017-01-10:13:55:55.555Z', '96f55c7d8f45', '120 USD', None], '68d84e5d1a57': ['2017-01-29:13:55:55.555Z', '96f55c7d8f45', '4.09 UwSD', None], '68d84e5d1a51': ['2017-01-28:13:55:55.555Z', '96f55c7d8f42', '36.34 USD', None]}, 'CUSTOMER': {'96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None], '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None]}, 'SITE_VISIT': {'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155022f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f54', {'some key': 'some value'}, None], 'ac05e8155029f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f57', {'some key': 'some value'}, None], 'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None], 'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None], 'ac05e8155012f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None], 'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None], 'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e815508f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None], 'ac05e8155013f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None], 'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None], 'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None], 'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None], 'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None], 'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None], 'ac05e8155023f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f55', {'some key': 'some value'}, None], 'ac05e8155010f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155015f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None], 'ac05e8155028f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f56', {'some key': 'some value'}, None]}}
test_Ingest_D2 = {'CUSTOMER':{},'SITE_VISIT':{},'IMAGE':{},'ORDER':{}}
test_Ingest_D2_result = {'CUSTOMER': {'96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55fg3ygyc7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Long', 'AK', None]}, 'IMAGE': {'d8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None], 'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None], 'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None]}, 'ORDER': {'68d84e5d1a47': ['2017-01-10:13:55:55.555Z', '96f55c7d8f45', '120 USD', None], '68d84e5d1a45': ['2017-01-10:13:55:55.555Z', '96f55c7d8f43', '9 USD', None], '68d84e5d1a48': ['2017-01-10:13:55:55.555Z', '96f55c7d8f46', '12 USD', None]}, 'SITE_VISIT': {'ac05e815508f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None], 'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None], 'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None]}}
test_Ingest_D3 = {'CUSTOMER':{},'SITE_VISIT':{},'IMAGE':{},'ORDER':{}}
test_Ingest_D3_result = {'CUSTOMER': {'96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None], '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None]}, 'ORDER': {}, 'IMAGE': {'d8ede43b1d18f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None], 'd8ede43b1d16f': ['2017-01-26:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None], 'd8ede43b1d10f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None], 'd8ede43b1d20f': ['2017-01-28:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None], 'd8ede43b1d34f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f56', 'Canon', 'EOS 80D', None], 'd8ede43b1d35f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f57', 'Canon', 'EOS 80D', None], 'd8ede43b1d22f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f49', 'Canon', 'EOS 80D', None], 'd8ede43b1d17f': ['2017-01-29:12:47:12.344Z', '96f55c7d8f43', 'Canon', 'EOS 80D', None], 'd8ede43b1d19f': ['2017-01-27:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None], 'd8ede43b1d15f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f47', 'Canon', 'EOS 80D', None], 'd8ede43b1d11f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f45', 'Canon', 'EOS 80D', None], 'd8ede43b1d14f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f46', 'Canon', 'EOS 80D', None], 'd8ede43b1d33f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f55', 'Canon', 'EOS 80D', None], 'd8ede43b1d32f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f50', 'Canon', 'EOS 80D', None], 'd8ede43b1d9f': ['2017-01-10:12:47:12.344Z', '96f55c7d8f42', 'Canon', 'EOS 80D', None]}, 'SITE_VISIT': {'ac05e8155020f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None], 'ac05e8155015f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None], 'ac05e8155021f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f50', {'some key': 'some value'}, None], 'ac05e815503f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155012f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e815509f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None], 'ac05e815506f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None], 'ac05e8155023f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f55', {'some key': 'some value'}, None], 'ac05e815508f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None], 'ac05e8155016f': ['2017-01-27:12:45:52.041Z', '96f55c7d8f46', {'some key': 'some value'}, None], 'ac05e8155019f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f49', {'some key': 'some value'}, None], 'ac05e815504f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155013f': ['2017-01-29:13:45:52.041Z', '96f55c7d8f43', {'some key': 'some value'}, None], 'ac05e8155028f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f56', {'some key': 'some value'}, None], 'ac05e8155018f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f48', {'some key': 'some value'}, None], 'ac05e8155017f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f47', {'some key': 'some value'}, None], 'ac05e815501f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e8155011f': ['2017-01-28:13:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e8155022f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f54', {'some key': 'some value'}, None], 'ac05e8155014f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f44', {'some key': 'some value'}, None], 'ac05e8155029f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f57', {'some key': 'some value'}, None], 'ac05e8155010f': ['2017-01-26:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None], 'ac05e815507f': ['2017-01-10:13:45:52.041Z', '96f55c7d8f45', {'some key': 'some value'}, None], 'ac05e815502f': ['2017-01-08:12:45:52.041Z', '96f55c7d8f42', {'some key': 'some value'}, None]}}
test_Ingest_D4 = {'CUSTOMER':{},'SITE_VISIT':{},'IMAGE':{},'ORDER':{}}
test_Ingest_D4_result = {'ORDER': {}, 'SITE_VISIT': {}, 'CUSTOMER': {'96f55c7d8f43': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f49': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'CA', None], '96f55c7d8f46': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f54': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f56': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f41': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f45': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f48': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f50': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f42': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f57': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f47': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None], '96f55c7d8f44': ['2017-01-06:12:46:46.384Z', 'Smith', 'Seattle', 'WA', None], '96f55c7d8f55': ['2017-01-06:12:46:46.384Z', 'Smith', 'Middletown', 'AK', None]}, 'IMAGE': {}}

#SHOW_ERROR_MESSAGES = False

class TestClass (unittest.TestCase):
    
    def testFileNotFound(self):
        # check if output file exists
        join_path = '../'
        path_to_files = os.path.join(os.path.dirname(__file__) ,join_path)
        os.chdir(path_to_files) 
        file = 'output/input.txt'
        file_path = os.path.join(path_to_files,file)          
        if os.path.isfile(file_path):
            print('File not found')
        
    def testTopXSimpleLTVCustomers(self):
        # ------ Tests for TopXSimpleLTVCustomers ------------
        join_path = '../'
        path_to_files = os.path.join(os.path.dirname(__file__) ,join_path)
        os.chdir(path_to_files) 
        file = 'output/output.txt'
        file_path = os.path.join(path_to_files,file)        
        
        # case 1: test 1 - general work check
        TopXSimpleLTVCustomers(10,test_D1)
        with open(file_path, 'r') as file:
            output = str(file.readlines()).strip("[]''")
        self.assertEqual(output,output_test1)
        
        # case 2: test 2 - check if ltv calculates properly
        # less customers, but keep some other data (site visits)
        # blank values in tables keys or un defined its names 
        # IMGE - NEW, ORDER - CREATE, CUSTOMER - ALTER
        TopXSimpleLTVCustomers(10,test_D2)
        with open(file_path, 'r') as file:
            output = str(file.readlines()).strip("[]''")
        self.assertEqual(output,output_test2)
        
        # case 3: test 3 - less site visits and orders
        TopXSimpleLTVCustomers(10,test_D3)
        with open(file_path, 'r') as file:
            output = str(file.readlines()).strip("[]''")
        self.assertEqual(output,output_test3)
        
        # case 4: test 4 - less orders
        TopXSimpleLTVCustomers(10,test_D4)
        with open(file_path, 'r') as file:
            output = str(file.readlines()).strip("[]''")
        self.assertEqual(output,output_test4)

    def testIngest(self):
        # ------ Tests for Ingest ------------
        #-------- case 1 ---------------------
        # original
        file = 'input_test1.txt'
        file_path = os.path.join(os.path.dirname(__file__),file)
        with open(file_path) as file_object:
            for line in file_object:
                if not line.isspace():
                    Ingest(ast.literal_eval(line.strip('\n\r[],')),test_Ingest_D1)
        self.assertEqual(test_Ingest_D1,test_Ingest_D1_result)
        
        #-------- case 2 ---------------------
        # less data
        file = 'input_test2.txt'
        file_path = os.path.join(os.path.dirname(__file__),file)
        with open(file_path) as file_object:
            for line in file_object:
                if not line.isspace():
                    Ingest(ast.literal_eval(line.strip('\n\r[],')),test_Ingest_D2)
        self.assertEqual(test_Ingest_D2,test_Ingest_D2_result)
        
        #-------- case 3 ---------------------
        # less no orders
        file = 'input_test3.txt'
        file_path = os.path.join(os.path.dirname(__file__),file)
        with open(file_path) as file_object:
            for line in file_object:
                if not line.isspace():
                    Ingest(ast.literal_eval(line.strip('\n\r[],')),test_Ingest_D3)
        self.assertEqual(test_Ingest_D3,test_Ingest_D3_result)
        #-------- case 4 ---------------------
        # less no customer activity
        file = 'input_test4.txt'
        file_path = os.path.join(os.path.dirname(__file__),file)
        with open(file_path) as file_object:
            for line in file_object:
                if not line.isspace():
                    Ingest(ast.literal_eval(line.strip('\n\r[],')),test_Ingest_D4)
        self.assertEqual(test_Ingest_D4,test_Ingest_D4_result)
        
        
if __name__ in '__main__':
    unittest.main(exit=False)