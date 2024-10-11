import unittest

from api_utils import *
from utils import SETTINGS, json


example_api_call = {
    "user_prompt": "List all open invoices for subsidiary DoorDash, Inc. Include all invoice columns including the subsidiary name.",
    "domain": "accounting",
    "subject_area": "AR",
    "entities_extracted": {
        "customer": "",
        "customerId": "",
        "subsidiary": "DoorDash, Inc.",
        "brand": "",
        "business_unit": "",
        "gl_account": "",
        "market": ""
    },
    "features_extracted": [
        {
            "type": "subsidiary",
            "index_name": "fincopilot-dim-subsidiary",
            "total_count": 4,
            "extracted_count": 110,
            "matched_on": "DoorDash, Inc.",
            "lookup_value": "DoorDash, Inc.",
            "matches": [
                {
                    "query_by_value": "c4ca4238a0b923820dcc509a6f75849b",
                    "lookup_value": "DoorDash, Inc.",
                    "system_id": 1,
                    "user_friendly_value": "DoorDash, Inc."
                },
                {
                    "query_by_value": "c20ad4d76fe97759aa27a0c99bff6710",
                    "lookup_value": "DoorDash Kitchens",
                    "system_id": 12,
                    "user_friendly_value": "DoorDash Kitchens"
                },
                {
                    "query_by_value": "ec8956637a99787bd197eacd77acce5e",
                    "lookup_value": "Wolt Oy US GAAP",
                    "system_id": 102,
                    "user_friendly_value": "Wolt Oy US GAAP"
                },
                {
                    "query_by_value": "eb160de1de89d9058fcb0b968dbbbd68",
                    "lookup_value": "DoorDash Technologies Brazil Ltd",
                    "system_id": 117,
                    "user_friendly_value": "DoorDash Technologies Brazil Ltd"
                },
                {
                    "query_by_value": "3416a75f4cea9109507cacd8e2f2aefc",
                    "lookup_value": "Rapid Retail Canada Inc.",
                    "system_id": 41,
                    "user_friendly_value": "Rapid Retail Canada Inc."
                },
                {
                    "query_by_value": "a1d0c6e83f027327d8461063f4ac58a6",
                    "lookup_value": "Wolt Enterprises Oy",
                    "system_id": 42,
                    "user_friendly_value": "Wolt Enterprises Oy"
                },
                {
                    "query_by_value": "7f39f8317fbdb1988ef4c628eba02591",
                    "lookup_value": "Wolt Services Oy",
                    "system_id": 61,
                    "user_friendly_value": "Wolt Services Oy"
                },
                {
                    "query_by_value": "c9f0f895fb98ab9159f51fd0297e236d",
                    "lookup_value": "Bixby Technologies, Inc.",
                    "system_id": 8,
                    "user_friendly_value": "Bixby Technologies, Inc."
                },
                {
                    "query_by_value": "c74d97b01eae257e44aa9d5bade97baf",
                    "lookup_value": "DashForce, Inc.",
                    "system_id": 16,
                    "user_friendly_value": "DashForce, Inc."
                },
                {
                    "query_by_value": "33e75ff09dd601bbe69f351039152189",
                    "lookup_value": "DoorDash Technologies Germany GmbH",
                    "system_id": 28,
                    "user_friendly_value": "DoorDash Technologies Germany GmbH"
                },
                {
                    "query_by_value": "6364d3f0f495b6ab9dcf8d3b5c6e0b01",
                    "lookup_value": "DoorDash Giftcards LLC",
                    "system_id": 32,
                    "user_friendly_value": "DoorDash Giftcards LLC"
                },
                {
                    "query_by_value": "e369853df766fa44e1ed0ff613f563bd",
                    "lookup_value": "DashCorps, Inc.",
                    "system_id": 34,
                    "user_friendly_value": "DashCorps, Inc."
                },
                {
                    "query_by_value": "1ff1de774005f8da13f42943881c655f",
                    "lookup_value": "DoorDash Essentials Canada Inc.",
                    "system_id": 24,
                    "user_friendly_value": "DoorDash Essentials Canada Inc."
                },
                {
                    "query_by_value": "7f6ffaa6bb0b408017b62254211691b5",
                    "lookup_value": "Wolt Austria GmbH",
                    "system_id": 112,
                    "user_friendly_value": "Wolt Austria GmbH"
                },
                {
                    "query_by_value": "642e92efb79421734881b53e1e1b18b6",
                    "lookup_value": "Wolt Danmark ApS",
                    "system_id": 48,
                    "user_friendly_value": "Wolt Danmark ApS"
                },
                {
                    "query_by_value": "f457c545a9ded88f18ecee47145a72c0",
                    "lookup_value": "Wolt Delivery Magyarország Kft.",
                    "system_id": 49,
                    "user_friendly_value": "Wolt Delivery Magyarország Kft."
                },
                {
                    "query_by_value": "da4fb5c6e93e74d3df8527599fa62642",
                    "lookup_value": "Wolt Texnologiyalari FE LLC",
                    "system_id": 120,
                    "user_friendly_value": "Wolt Texnologiyalari FE LLC"
                },
                {
                    "query_by_value": "72b32a1f754ba1c09b3695e0cb6cde7f",
                    "lookup_value": "Wolt Magyarország Kft",
                    "system_id": 57,
                    "user_friendly_value": "Wolt Magyarország Kft"
                },
                {
                    "query_by_value": "f033ab37c30201f73f142449d037028d",
                    "lookup_value": "Wolt Technologies Greece Provision of food services S.A.",
                    "system_id": 80,
                    "user_friendly_value": "Wolt Technologies Greece Provision of food services S.A."
                },
                {
                    "query_by_value": "8f14e45fceea167a5a36dedd4bea2543",
                    "lookup_value": "ScottyLabs",
                    "system_id": 7,
                    "user_friendly_value": "ScottyLabs"
                },
                {
                    "query_by_value": "37693cfc748049e45d87b8c7d8b9aacd",
                    "lookup_value": "Agora Insurance, Inc",
                    "system_id": 23,
                    "user_friendly_value": "Agora Insurance, Inc"
                },
                {
                    "query_by_value": "19ca14e7ea6328a42e0eb13d585e4c22",
                    "lookup_value": "DoorDash Technologies New Zealand",
                    "system_id": 36,
                    "user_friendly_value": "DoorDash Technologies New Zealand"
                },
                {
                    "query_by_value": "5fd0b37cd7dbbb00f97ba6ce92bf5add",
                    "lookup_value": "Wolt Enterprises Iceland ehf",
                    "system_id": 114,
                    "user_friendly_value": "Wolt Enterprises Iceland ehf"
                },
                {
                    "query_by_value": "17e62166fc8586dfa4d1bc0e1742c08b",
                    "lookup_value": "Elimination - Oy",
                    "system_id": 43,
                    "user_friendly_value": "Elimination - Oy"
                },
                {
                    "query_by_value": "c0c7c76d30bd3dcaefc96f40275bdc0a",
                    "lookup_value": "Wolt Eesti Oü",
                    "system_id": 50,
                    "user_friendly_value": "Wolt Eesti Oü"
                },
                {
                    "query_by_value": "a684eceee76fc522773286a895bc8436",
                    "lookup_value": "Wolt Latvija SIA",
                    "system_id": 54,
                    "user_friendly_value": "Wolt Latvija SIA"
                },
                {
                    "query_by_value": "35f4a8d465e6e1edc05f3d8ab658c551",
                    "lookup_value": "Wolt Slovensko s. r. o.",
                    "system_id": 78,
                    "user_friendly_value": "Wolt Slovensko s. r. o."
                },
                {
                    "query_by_value": "68d30a9594728bc39aa24be94b319d21",
                    "lookup_value": "Wolt Česko s.r.o.",
                    "system_id": 84,
                    "user_friendly_value": "Wolt Česko s.r.o."
                },
                {
                    "query_by_value": "e4da3b7fbbce2345d7772b0674a318d5",
                    "lookup_value": "DoorDash Essentials, LLC",
                    "system_id": 5,
                    "user_friendly_value": "DoorDash Essentials, LLC"
                },
                {
                    "query_by_value": "93db85ed909c13838ff95ccfa94cebd9",
                    "lookup_value": "Wolt Enterprises Israel LTD",
                    "system_id": 86,
                    "user_friendly_value": "Wolt Enterprises Israel LTD"
                },
                {
                    "query_by_value": "44f683a84163b3523afe57c2e008bc8c",
                    "lookup_value": "Elimination - Services Oy",
                    "system_id": 62,
                    "user_friendly_value": "Elimination - Services Oy"
                },
                {
                    "query_by_value": "735b90b4568125ed6c3f678819b6e058",
                    "lookup_value": "Wolt Services Deutschland GmbH",
                    "system_id": 67,
                    "user_friendly_value": "Wolt Services Deutschland GmbH"
                },
                {
                    "query_by_value": "e2c420d928d4bf8ce0ff2ec19b371514",
                    "lookup_value": "Wolt Services Magyarország Kft",
                    "system_id": 71,
                    "user_friendly_value": "Wolt Services Magyarország Kft"
                },
                {
                    "query_by_value": "ad61ab143223efbc24c7d2583be69251",
                    "lookup_value": "Wolt Services Sverige AB",
                    "system_id": 74,
                    "user_friendly_value": "Wolt Services Sverige AB"
                },
                {
                    "query_by_value": "28dd2c7955ce926456240b2ff0100bde",
                    "lookup_value": "Wolt Services d.o.o. Ljubljana",
                    "system_id": 77,
                    "user_friendly_value": "Wolt Services d.o.o. Ljubljana"
                },
                {
                    "query_by_value": "43ec517d68b6edd3015b3edc9a11367b",
                    "lookup_value": "Wolt Market Greece Single Member Private Company",
                    "system_id": 81,
                    "user_friendly_value": "Wolt Market Greece Single Member Private Company"
                },
                {
                    "query_by_value": "eccbc87e4b5ce2fe28308fd9f2a7baf3",
                    "lookup_value": "Elimination - DoorDash, Inc.",
                    "system_id": 3,
                    "user_friendly_value": "Elimination - DoorDash, Inc."
                },
                {
                    "query_by_value": "a87ff679a2f3e71d9181a67b7542122c",
                    "lookup_value": "DoorDash Technologies Australia Pty Ltd",
                    "system_id": 4,
                    "user_friendly_value": "DoorDash Technologies Australia Pty Ltd"
                },
                {
                    "query_by_value": "1679091c5a880faf6fb5e6087eb1b2dc",
                    "lookup_value": "Caviar",
                    "system_id": 6,
                    "user_friendly_value": "Caviar"
                },
                {
                    "query_by_value": "6512bd43d9caa6e02c990b0a82652dca",
                    "lookup_value": "DoorDash Technologies Puerto Rico, LLC",
                    "system_id": 11,
                    "user_friendly_value": "DoorDash Technologies Puerto Rico, LLC"
                },
                {
                    "query_by_value": "aab3238922bcc25a6f606eb525ffdc56",
                    "lookup_value": "DoorDash Technologies Japan Ltd.",
                    "system_id": 14,
                    "user_friendly_value": "DoorDash Technologies Japan Ltd."
                },
                {
                    "query_by_value": "9bf31c7ff062936a96d3c8bd1f8f2ff3",
                    "lookup_value": "DDPW20",
                    "system_id": 15,
                    "user_friendly_value": "DDPW20"
                },
                {
                    "query_by_value": "70efdf2ec9b086079795c442636b55fb",
                    "lookup_value": "Chowbotics, Inc.",
                    "system_id": 17,
                    "user_friendly_value": "Chowbotics, Inc."
                },
                {
                    "query_by_value": "4e732ced3463d06de0ca9a15b6153677",
                    "lookup_value": "DoorDash Essentials HoldCo",
                    "system_id": 26,
                    "user_friendly_value": "DoorDash Essentials HoldCo"
                },
                {
                    "query_by_value": "34173cb38f07f89ddbebc2ac9128303f",
                    "lookup_value": "Doordash G&C, LLC",
                    "system_id": 30,
                    "user_friendly_value": "Doordash G&C, LLC"
                },
                {
                    "query_by_value": "d67d8ab4f4c10bf22aa353e27879133c",
                    "lookup_value": "Bbot, Inc.",
                    "system_id": 39,
                    "user_friendly_value": "Bbot, Inc."
                },
                {
                    "query_by_value": "98f13708210194c475687be6106a3b84",
                    "lookup_value": "Chowbotics (Dongguan) Robot Technology Co., Ltd.",
                    "system_id": 20,
                    "user_friendly_value": "Chowbotics (Dongguan) Robot Technology Co., Ltd."
                },
                {
                    "query_by_value": "6ea9ab1baa0efb9e19094440c317e21b",
                    "lookup_value": "Elimination - Chowbotics, Inc.",
                    "system_id": 29,
                    "user_friendly_value": "Elimination - Chowbotics, Inc."
                },
                {
                    "query_by_value": "b6d767d2f8ed5d21a44b0e5886680cb9",
                    "lookup_value": "DoorDash Technologies India Private Limited",
                    "system_id": 22,
                    "user_friendly_value": "DoorDash Technologies India Private Limited"
                },
                {
                    "query_by_value": "02e74f10e0327ad868d138f2b4fdd6f0",
                    "lookup_value": "DoorDash Essentials LLC, Australia",
                    "system_id": 27,
                    "user_friendly_value": "DoorDash Essentials LLC, Australia"
                },
                {
                    "query_by_value": "c16a5320fa475530d9583c34fd356ef5",
                    "lookup_value": "DoorDash Essentials Germany GmbH",
                    "system_id": 31,
                    "user_friendly_value": "DoorDash Essentials Germany GmbH"
                },
                {
                    "query_by_value": "a5bfc9e07964f8dddeb95fc584cd965d",
                    "lookup_value": "DashCorps Services, Inc.",
                    "system_id": 37,
                    "user_friendly_value": "DashCorps Services, Inc."
                },
                {
                    "query_by_value": "2b44928ae11fb9384c4cf38708677c48",
                    "lookup_value": "Wolt Luxembourg S.à r.l.",
                    "system_id": 115,
                    "user_friendly_value": "Wolt Luxembourg S.à r.l."
                },
                {
                    "query_by_value": "6c8349cc7260ae62e3b1396831a8398f",
                    "lookup_value": "Wolt Azerbaijan Limited Liability Company",
                    "system_id": 45,
                    "user_friendly_value": "Wolt Azerbaijan Limited Liability Company"
                },
                {
                    "query_by_value": "d82c8d1619ad8176d665453cfb2e55f0",
                    "lookup_value": "Wolt Operations Oy",
                    "system_id": 53,
                    "user_friendly_value": "Wolt Operations Oy"
                },
                {
                    "query_by_value": "9f61408e3afb633e50cdf1b20de6f466",
                    "lookup_value": "Wolt Logistics Europe sp. z.o.o",
                    "system_id": 56,
                    "user_friendly_value": "Wolt Logistics Europe sp. z.o.o"
                },
                {
                    "query_by_value": "66f041e16a60928b05a7e228a89c3799",
                    "lookup_value": "Wolt Malta Limited",
                    "system_id": 58,
                    "user_friendly_value": "Wolt Malta Limited"
                },
                {
                    "query_by_value": "093f65e080a295f8076b1c5722a46aa2",
                    "lookup_value": "Wolt Norway AS",
                    "system_id": 59,
                    "user_friendly_value": "Wolt Norway AS"
                },
                {
                    "query_by_value": "d1fe173d08e959397adf34b1d77e88d7",
                    "lookup_value": "Wolt Sverige AB",
                    "system_id": 79,
                    "user_friendly_value": "Wolt Sverige AB"
                },
                {
                    "query_by_value": "9778d5d219c5080b9a6a17bef029331c",
                    "lookup_value": "Wolt Technologies Kazakhstan LLP",
                    "system_id": 82,
                    "user_friendly_value": "Wolt Technologies Kazakhstan LLP"
                },
                {
                    "query_by_value": "2a38a4a9316c49e5a833517c45d31070",
                    "lookup_value": "Wolt Development Deutschland GmbH",
                    "system_id": 88,
                    "user_friendly_value": "Wolt Development Deutschland GmbH"
                },
                {
                    "query_by_value": "ed3d2c21991e3bef5e069713af9fa6ca",
                    "lookup_value": "Wolt Enterprises Deutschland GmbH",
                    "system_id": 98,
                    "user_friendly_value": "Wolt Enterprises Deutschland GmbH"
                },
                {
                    "query_by_value": "ac627ab1ccbdb62ec96e702f07f6425b",
                    "lookup_value": "UAB Wolt LT",
                    "system_id": 99,
                    "user_friendly_value": "UAB Wolt LT"
                },
                {
                    "query_by_value": "f899139df5e1059396431415e770c6dd",
                    "lookup_value": "Wolt Japan KK",
                    "system_id": 100,
                    "user_friendly_value": "Wolt Japan KK"
                },
                {
                    "query_by_value": "6974ce5ac660610b44d9b9fed0ff9548",
                    "lookup_value": "Wolt Oy USD",
                    "system_id": 103,
                    "user_friendly_value": "Wolt Oy USD"
                },
                {
                    "query_by_value": "c9e1074f5b3f9fc8ea15d152add07294",
                    "lookup_value": "Wolt Development Sverige AB",
                    "system_id": 104,
                    "user_friendly_value": "Wolt Development Sverige AB"
                },
                {
                    "query_by_value": "65b9eea6e1cc6bb9f0cd2a47751a186f",
                    "lookup_value": "Wolt Development Netherlands B.V.",
                    "system_id": 105,
                    "user_friendly_value": "Wolt Development Netherlands B.V."
                },
                {
                    "query_by_value": "f0935e4cd5920aa6c7c996a5ee53a70f",
                    "lookup_value": "Wolt Logistics Deutschland GmbH",
                    "system_id": 106,
                    "user_friendly_value": "Wolt Logistics Deutschland GmbH"
                },
                {
                    "query_by_value": "a97da629b098b75c294dffdc3e463904",
                    "lookup_value": "Wolt Development UK Ltd",
                    "system_id": 107,
                    "user_friendly_value": "Wolt Development UK Ltd"
                },
                {
                    "query_by_value": "a3c65c2974270fd093ee8a9bf8ae7d0b",
                    "lookup_value": "Wolt Zagreb d.o.o. [EUR]",
                    "system_id": 108,
                    "user_friendly_value": "Wolt Zagreb d.o.o. [EUR]"
                },
                {
                    "query_by_value": "ea5d2f1c4608232e07d3aa3d998e5135",
                    "lookup_value": "Wolt Operations Services Israel Ltd.",
                    "system_id": 64,
                    "user_friendly_value": "Wolt Operations Services Israel Ltd."
                },
                {
                    "query_by_value": "fc490ca45c00b1249bbe3554a4fdf6fb",
                    "lookup_value": "Wolt Services Eesti OÜ",
                    "system_id": 65,
                    "user_friendly_value": "Wolt Services Eesti OÜ"
                },
                {
                    "query_by_value": "3295c76acbf4caaed33c36b1b5fc2cb1",
                    "lookup_value": "Wolt Services Danmark ApS",
                    "system_id": 66,
                    "user_friendly_value": "Wolt Services Danmark ApS"
                },
                {
                    "query_by_value": "14bfa6bb14875e45bba028a21ed38046",
                    "lookup_value": "Wolt Services Georgia LLC",
                    "system_id": 69,
                    "user_friendly_value": "Wolt Services Georgia LLC"
                },
                {
                    "query_by_value": "32bb90e8976aab5298d5da10fe66f21d",
                    "lookup_value": "Wolt Services Polska sp. z o.o.",
                    "system_id": 72,
                    "user_friendly_value": "Wolt Services Polska sp. z o.o."
                },
                {
                    "query_by_value": "d2ddea18f00665ce8623e36bd4e3c7c5",
                    "lookup_value": "Wolt Services Slovensko s. r. o.",
                    "system_id": 73,
                    "user_friendly_value": "Wolt Services Slovensko s. r. o."
                },
                {
                    "query_by_value": "c7e1249ffc03eb9ded908c236bd1996d",
                    "lookup_value": "Wolt Services d.o.o. Beograd",
                    "system_id": 87,
                    "user_friendly_value": "Wolt Services d.o.o. Beograd"
                },
                {
                    "query_by_value": "54229abfcfa5649e7003b83dd4755294",
                    "lookup_value": "Wolt Services Azerbaijan LLC",
                    "system_id": 91,
                    "user_friendly_value": "Wolt Services Azerbaijan LLC"
                },
                {
                    "query_by_value": "f4b9ec30ad9f68f89b29639786cb62ef",
                    "lookup_value": "Wolt Services Malta Limited",
                    "system_id": 94,
                    "user_friendly_value": "Wolt Services Malta Limited"
                },
                {
                    "query_by_value": "812b4ba287f5ee0bc9d43bbf5bbe87fb",
                    "lookup_value": "Wolt Services Kazakhstan LLP",
                    "system_id": 95,
                    "user_friendly_value": "Wolt Services Kazakhstan LLP"
                },
                {
                    "query_by_value": "38b3eff8baf56627478ec76a704e9b52",
                    "lookup_value": "Wolt Market Japan K.K.",
                    "system_id": 101,
                    "user_friendly_value": "Wolt Market Japan K.K."
                },
                {
                    "query_by_value": "c45147dee729311ef5b5c3003946c48f",
                    "lookup_value": "Wolt Israel Eilat",
                    "system_id": 116,
                    "user_friendly_value": "Wolt Israel Eilat"
                },
                {
                    "query_by_value": "c81e728d9d4c2f636f067f89cc14862c",
                    "lookup_value": "DoorDash Technologies Canada, Inc.",
                    "system_id": 2,
                    "user_friendly_value": "DoorDash Technologies Canada, Inc."
                },
                {
                    "query_by_value": "182be0c5cdcd5072bb1864cdee4d3d6e",
                    "lookup_value": "DoorDash Essentials Logistics, LLC",
                    "system_id": 33,
                    "user_friendly_value": "DoorDash Essentials Logistics, LLC"
                },
                {
                    "query_by_value": "d645920e395fedad7bbbed0eca3fe2e0",
                    "lookup_value": "DashLink Inc",
                    "system_id": 40,
                    "user_friendly_value": "DashLink Inc"
                },
                {
                    "query_by_value": "d9d4f495e875a2e075a1a4a6e1b9770f",
                    "lookup_value": "Wolt Cyprus Limited",
                    "system_id": 46,
                    "user_friendly_value": "Wolt Cyprus Limited"
                },
                {
                    "query_by_value": "b53b3a3d6ab90ce0268229151c9bde11",
                    "lookup_value": "Wolt License Services Oy",
                    "system_id": 55,
                    "user_friendly_value": "Wolt License Services Oy"
                },
                {
                    "query_by_value": "fe9fc289c3ff0af142b6d3bead98a923",
                    "lookup_value": "Wolt Zagreb d.o.o.",
                    "system_id": 83,
                    "user_friendly_value": "Wolt Zagreb d.o.o."
                },
                {
                    "query_by_value": "7647966b7343c29048673252e490f736",
                    "lookup_value": "Wolt Enterprises B.V.",
                    "system_id": 89,
                    "user_friendly_value": "Wolt Enterprises B.V."
                },
                {
                    "query_by_value": "26657d5ff9020d2abefe558796b99584",
                    "lookup_value": "Wolt Development Eesti OÜ",
                    "system_id": 96,
                    "user_friendly_value": "Wolt Development Eesti OÜ"
                },
                {
                    "query_by_value": "7cbbc409ec990f19c78c75bd1e06f215",
                    "lookup_value": "Wolt Services Latvija SIA",
                    "system_id": 70,
                    "user_friendly_value": "Wolt Services Latvija SIA"
                },
                {
                    "query_by_value": "072b030ba126b2f4b2374f342be9ed44",
                    "lookup_value": "Wolt Polska Sp.z.o.o.",
                    "system_id": 60,
                    "user_friendly_value": "Wolt Polska Sp.z.o.o."
                },
                {
                    "query_by_value": "03afdbd66e7929b125f8597834fa83a4",
                    "lookup_value": "UAB Wolt Services LT",
                    "system_id": 63,
                    "user_friendly_value": "UAB Wolt Services LT"
                },
                {
                    "query_by_value": "c51ce410c124a10e0db5e4b97fc2af39",
                    "lookup_value": "DoorDash Technologies Mexico",
                    "system_id": 13,
                    "user_friendly_value": "DoorDash Technologies Mexico"
                },
                {
                    "query_by_value": "07e1cd7dca89a1678042477183b7ac3f",
                    "lookup_value": "Wolt Albania SHPK",
                    "system_id": 119,
                    "user_friendly_value": "Wolt Albania SHPK"
                },
                {
                    "query_by_value": "73278a4a86960eeb576a8fd4c9ec6997",
                    "lookup_value": "Wolt Development Israel LTD",
                    "system_id": 113,
                    "user_friendly_value": "Wolt Development Israel LTD"
                },
                {
                    "query_by_value": "67c6a1e7ce56d3d6fa748ab6d9af3fd7",
                    "lookup_value": "Wolt d.o.o. Beograd - Savski venac",
                    "system_id": 47,
                    "user_friendly_value": "Wolt d.o.o. Beograd - Savski venac"
                },
                {
                    "query_by_value": "3ef815416f775098fe977004015c6193",
                    "lookup_value": "Wolt, tehnologije d.o.o. Ljubljana",
                    "system_id": 85,
                    "user_friendly_value": "Wolt, tehnologije d.o.o. Ljubljana"
                },
                {
                    "query_by_value": "e2ef524fbf3d9fe611d5a8e90fefdc9c",
                    "lookup_value": "Wolt Development Danmark ApS",
                    "system_id": 97,
                    "user_friendly_value": "Wolt Development Danmark ApS"
                },
                {
                    "query_by_value": "d09bf41544a3365a46c9077ebb5e35c3",
                    "lookup_value": "Wolt Services Zagreb d.o.o.",
                    "system_id": 75,
                    "user_friendly_value": "Wolt Services Zagreb d.o.o."
                },
                {
                    "query_by_value": "fbd7939d674997cdb4692d34de8633c4",
                    "lookup_value": "Wolt Services Česko s.r.o.",
                    "system_id": 76,
                    "user_friendly_value": "Wolt Services Česko s.r.o."
                },
                {
                    "query_by_value": "92cc227532d17e56e07902b254dfad10",
                    "lookup_value": "Wolt Services Cyprus Limited",
                    "system_id": 92,
                    "user_friendly_value": "Wolt Services Cyprus Limited"
                },
                {
                    "query_by_value": "98dce83da57b0395e163467c9dae521b",
                    "lookup_value": "Wolt Services Norway AS",
                    "system_id": 93,
                    "user_friendly_value": "Wolt Services Norway AS"
                },
                {
                    "query_by_value": "5f93f983524def3dca464469d2cf9f3e",
                    "lookup_value": "Wolt Services Zagreb d.o.o. [EUR]",
                    "system_id": 110,
                    "user_friendly_value": "Wolt Services Zagreb d.o.o. [EUR]"
                },
                {
                    "query_by_value": "9a1158154dfa42caddbd0694a4e9bdc8",
                    "lookup_value": "Wolt Georgia LLC",
                    "system_id": 52,
                    "user_friendly_value": "Wolt Georgia LLC"
                },
                {
                    "query_by_value": "a5771bce93e200c36f7cd9dfd0e5deaa",
                    "lookup_value": "DoorDash Pharmacy Services, LLC",
                    "system_id": 38,
                    "user_friendly_value": "DoorDash Pharmacy Services, LLC"
                },
                {
                    "query_by_value": "5ef059938ba799aaa845e1c2e8a762bd",
                    "lookup_value": "1P Market Deutschland GmbH",
                    "system_id": 118,
                    "user_friendly_value": "1P Market Deutschland GmbH"
                },
                {
                    "query_by_value": "c81e728d9d4c2f636f067f89cc14862c",
                    "lookup_value": "DoorDash Technologies Canada, Inc.",
                    "system_id": 2,
                    "user_friendly_value": "DoorDash Technologies Canada, Inc."
                },
                {
                    "query_by_value": "eccbc87e4b5ce2fe28308fd9f2a7baf3",
                    "lookup_value": "Elimination - DoorDash, Inc.",
                    "system_id": 3,
                    "user_friendly_value": "Elimination - DoorDash, Inc."
                },
                {
                    "query_by_value": "1ff1de774005f8da13f42943881c655f",
                    "lookup_value": "DoorDash Essentials Canada Inc.",
                    "system_id": 24,
                    "user_friendly_value": "DoorDash Essentials Canada Inc."
                }
            ]
        }
    ]
}


with open(SETTINGS['chatbot']['context_schema_file']) as f:
    llm_context_schema = json.load(f)



class TestAPIUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_key_checker(self):
        logger.debug("Testing key checker")

        logger.debug(f"keys in example: {example_api_call.keys()}")
        
    def test_get_table_info_from_table_name(self):
        logger.debug(f"table_info result {get_table_info_from_table_name(['ar_credit_memo', 'ar_invoice', 'ar_customer'], 5)}")
        pass

    def test_input_parser(self):
        response = input_parser(example_api_call)

        logger.debug(f"input parser response:{response}")

        # For user_prompt not present
        with self.assertRaises(SQLGenerationException) as context:
            input_parser(event={
                'user_question': "What is that thing?"
            })

            self.assertTrue(context.exception.reason == Reason.INVALID_API_CALL)
        

    def test_output_validator(self):
        pass

    def test_response_builder(self):
        pass



def main():
    unittest.main()

if __name__ == '__main__':
    main()