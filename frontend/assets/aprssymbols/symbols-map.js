/* This file is just a breakout of each APRS symbol from the primary and secondary symbol tables 
* It provides a convient way to reference/find an APRS symbol png file.
*
*/

// symbolRotation provides a list of those APRS symbol codes that are allowed to be rotated.
// origin_degrees - The direction an image is "pointing" within the PNG file.  
// flip - if true, then an image should be flipped in order to appear right side up when reporting a bearing > 180 degrees...and the png to use should end with xxxx-flip.png
// alternate_table - if true, then this symbol code has an APRS icon from the alternate table as well.
//
// If an APRS object is reporting a bearing > 180 degrees, then the image might need be flipped so it appears "right side up".
var symbolRotation = {
    "'" : { "origin_degrees" :  0, "flip" : "true", "alternate_table" : "false" },
    "(" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "*" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" },
    "<" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "=" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    ">" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "C" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "F" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "P" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "U" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "X" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "Y" : { "origin_degrees" : 270, "flip" : "true", "alternate_table" : "false" }, 
    "[" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "^" : { "origin_degrees" :  0, "flip" : "false","alternate_table" : "true" }, 
    "a" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "b" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "e" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "f" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "g" : { "origin_degrees" : 270, "flip" : "true", "alternate_table" : "false" }, 
    "j" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "k" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "true" }, 
    "p" : { "origin_degrees" : 270, "flip" : "true", "alternate_table" : "false" }, 
    "s" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "false" }, 
    "u" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "true" }, 
    "v" : { "origin_degrees" : 90, "flip" : "true", "alternate_table" : "true" }
};


var symbols = {
      "/U" : {
         "identity" : "PU",
         "description" : "Bus"
      },
      "\\_" : {
         "identity" : "DW",
         "description" : "Weather site"
      },
      "/K" : {
         "identity" : "PK",
         "description" : "School"
      },
      "\\&" : {
         "identity" : "OG",
         "description" : "Gateway station"
      },
      "\\n" : {
         "identity" : "SN",
         "description" : "Red triangle"
      },
      "/)" : {
         "identity" : "BJ",
         "description" : "Wheelchair, handicapped"
      },
      "/u" : {
         "identity" : "LU",
         "description" : "Semi-trailer truck, 18-wheeler"
      },
      "/X" : {
         "identity" : "PX",
         "description" : "Helicopter"
      },
      "\\U" : {
         "identity" : "AU",
         "description" : "Sunny"
      },
      "\\]" : {
         "identity" : "DU",
         "unused" : 1
      },
      "/D" : {
         "identity" : "PD",
         "unused" : 1
      },
      "/0" : {
         "identity" : "P0",
         "description" : "Numbered circle: 0"
      },
      "/#" : {
         "identity" : "BD",
         "description" : "Digipeater"
      },
      "\\j" : {
         "identity" : "SJ",
         "description" : "Work zone, excavating machine"
      },
      "\\e" : {
         "identity" : "SE",
         "description" : "Sleet"
      },
      "\\." : {
         "identity" : "OO",
         "description" : "Ambiguous, question mark inside circle"
      },
      "/g" : {
         "identity" : "LG",
         "description" : "Glider"
      },
      "/=" : {
         "identity" : "MU",
         "description" : "Railroad engine"
      },
      "\\w" : {
         "identity" : "SW",
         "description" : "Flooding"
      },
      "/B" : {
         "identity" : "PB",
         "description" : "BBS"
      },
      "/A" : {
         "identity" : "PA",
         "description" : "Aid station"
      },
      "\\(" : {
         "identity" : "OI",
         "description" : "Cloudy"
      },
      "/;" : {
         "identity" : "MS",
         "description" : "Campground, tent"
      },
      "\\<" : {
         "identity" : "NT",
         "description" : "Advisory, single red flag"
      },
      "\\k" : {
         "identity" : "SK",
         "description" : "SUV, ATV"
      },
      "\\W" : {
         "identity" : "AW",
         "description" : "NWS site"
      },
      "\\8" : {
         "identity" : "A8",
         "description" : "802.11 WiFi or other network node"
      },
      "\\%" : {
         "identity" : "OF",
         "unused" : 1
      },
      "/'" : {
         "identity" : "BH",
         "description" : "Small aircraft"
      },
      "/@" : {
         "identity" : "MX",
         "description" : "Hurricane predicted path"
      },
      "\\Y" : {
         "identity" : "AY",
         "unused" : 1
      },
      "/," : {
         "identity" : "BM",
         "description" : "Boy Scouts"
      },
      "/7" : {
         "identity" : "P7",
         "description" : "Numbered circle: 7"
      },
      "/H" : {
         "identity" : "PH",
         "description" : "Hotel"
      },
      "/." : {
         "identity" : "BO",
         "description" : "Red X"
      },
      "\\E" : {
         "identity" : "AE",
         "description" : "Smoke, Chimney"
      },
      "\\b" : {
         "identity" : "SB",
         "description" : "Blowing dust, sand"
      },
      "\\4" : {
         "identity" : "A4",
         "unused" : 1
      },
      "/i" : {
         "identity" : "LI",
         "description" : "IOTA, islands on the air"
      },
      "/-" : {
         "identity" : "BN",
         "description" : "House"
      },
      "\\A" : {
         "identity" : "AA",
         "description" : "White box"
      },
      "\\@" : {
         "identity" : "NX",
         "description" : "Hurricane, Tropical storm"
      },
      "\\Q" : {
         "identity" : "AQ",
         "description" : "Earthquake"
      },
      "/j" : {
         "identity" : "LJ",
         "description" : "Jeep"
      },
      "\\F" : {
         "identity" : "AF",
         "description" : "Freezing rain"
      },
      "\\#" : {
         "identity" : "OD",
         "description" : "Digipeater, green star"
      },
      "/G" : {
         "identity" : "PG",
         "description" : "Grid square, 3 by 3"
      },
      "\\g" : {
         "identity" : "SG",
         "description" : "Gale, two red flags"
      },
      "\\7" : {
         "identity" : "A7",
         "unused" : 1
      },
      "\\S" : {
         "identity" : "AS",
         "description" : "Satellite"
      },
      "/n" : {
         "identity" : "LN",
         "description" : "Node, black bulls-eye"
      },
      "/E" : {
         "identity" : "PE",
         "description" : "Eyeball"
      },
      "/9" : {
         "identity" : "P9",
         "description" : "Numbered circle: 9"
      },
      "\\9" : {
         "identity" : "A9",
         "description" : "Gas station"
      },
      "\\c" : {
         "identity" : "SC",
         "description" : "CD triangle, RACES, CERTS, SATERN"
      },
      "\\>" : {
         "identity" : "NV",
         "description" : "Red car"
      },
      "/?" : {
         "identity" : "MW",
         "description" : "File server"
      },
      "\\r" : {
         "identity" : "SR",
         "description" : "Restrooms"
      },
      "/Y" : {
         "identity" : "PY",
         "description" : "Sailboat"
      },
      "/b" : {
         "identity" : "LB",
         "description" : "Bicycle"
      },
      "\\a" : {
         "identity" : "SA",
         "description" : "Red diamond"
      },
      "\\O" : {
         "identity" : "AO",
         "description" : "Rocket"
      },
      "\\-" : {
         "identity" : "ON",
         "description" : "House, HF antenna"
      },
      "/T" : {
         "identity" : "PT",
         "description" : "SSTV"
      },
      "\\h" : {
         "identity" : "SH",
         "description" : "Store"
      },
      "\\B" : {
         "identity" : "AB",
         "description" : "Blowing snow"
      },
      "/k" : {
         "identity" : "LK",
         "description" : "Truck"
      },
      "/*" : {
         "identity" : "BK",
         "description" : "Snowmobile"
      },
      "\\+" : {
         "identity" : "OL",
         "description" : "Church"
      },
      "/\"" : {
         "identity" : "BC",
         "unused" : 1
      },
      "/{" : {
         "identity" : "J1",
         "unused" : 1
      },
      "/|" : {
         "identity" : "J2",
         "unused" : 1
      },
      "\\|" : {
         "identity" : "Q2",
         "unused" : 1
      },
      "/~" : {
         "identity" : "J4",
         "unused" : 1
      },
      "\\~" : {
         "identity" : "Q4",
         "unused" : 1
      },
      "/R" : {
         "identity" : "PR",
         "description" : "Recreational vehicle"
      },
      "/\\" : {
         "identity" : "HT",
         "description" : "DF triangle"
      },
      "\\C" : {
         "identity" : "AC",
         "description" : "Coast Guard"
      },
      "\\p" : {
         "identity" : "SP",
         "description" : "Partly cloudy"
      },
      "\\I" : {
         "identity" : "AI",
         "description" : "Rain shower"
      },
      "/v" : {
         "identity" : "LV",
         "description" : "Van"
      },
      "/Q" : {
         "identity" : "PQ",
         "unused" : 1
      },
      "\\*" : {
         "identity" : "OK",
         "description" : "Snow"
      },
      "/V" : {
         "identity" : "PV",
         "description" : "ATV, Amateur Television"
      },
      "\\R" : {
         "identity" : "AR",
         "description" : "Restaurant"
      },
      "/^" : {
         "identity" : "HV",
         "description" : "Large aircraft"
      },
      "/s" : {
         "identity" : "LS",
         "description" : "Ship, power boat"
      },
      "\\t" : {
         "identity" : "ST",
         "description" : "Tornado"
      },
      "/}" : {
         "identity" : "J3",
         "unused" : 1
      },
      "/p" : {
         "identity" : "LP",
         "description" : "Dog"
      },
      "/`" : {
         "identity" : "HX",
         "description" : "Satellite dish antenna"
      },
      "\\V" : {
         "identity" : "AV",
         "description" : "VORTAC, Navigational aid"
      },
      "/M" : {
         "identity" : "PM",
         "description" : "Mac apple"
      },
      "/Z" : {
         "identity" : "PZ",
         "description" : "Windows flag"
      },
      "\\0" : {
         "identity" : "A0",
         "description" : "Circle, IRLP / Echolink/WIRES"
      },
      "\\2" : {
         "identity" : "A2",
         "unused" : 1
      },
      "/r" : {
         "identity" : "LR",
         "description" : "Repeater tower"
      },
      "//" : {
         "identity" : "BP",
         "description" : "Red dot"
      },
      "\\K" : {
         "identity" : "AK",
         "description" : "Kenwood HT"
      },
      "\\1" : {
         "identity" : "A1",
         "unused" : 1
      },
      "\\v" : {
         "identity" : "SV",
         "description" : "Van"
      },
      "\\\\" : {
         "identity" : "DT",
         "unused" : 1
      },
      "/I" : {
         "identity" : "PI",
         "description" : "TCP/IP network station"
      },
      "/h" : {
         "identity" : "LH",
         "description" : "Hospital"
      },
      "\\P" : {
         "identity" : "AP",
         "description" : "Parking"
      },
      "\\u" : {
         "identity" : "SU",
         "description" : "No. Truck"
      },
      "/:" : {
         "identity" : "MR",
         "description" : "Fire"
      },
      "\\'" : {
         "identity" : "OH",
         "description" : "Crash / incident site"
      },
      "\\X" : {
         "identity" : "AX",
         "description" : "Pharmacy"
      },
      "\\)" : {
         "identity" : "OJ",
         "description" : "Firenet MEO, MODIS Earth Observation"
      },
      "/&" : {
         "identity" : "BG",
         "description" : "HF gateway"
      },
      "/x" : {
         "identity" : "LX",
         "description" : "X / Unix"
      },
      "\\5" : {
         "identity" : "A5",
         "unused" : 1
      },
      "/W" : {
         "identity" : "PW",
         "description" : "Weather service site"
      },
      "\\^" : {
         "identity" : "DV",
         "description" : "Aircraft"
      },
      "/8" : {
         "identity" : "P8",
         "description" : "Numbered circle: 8"
      },
      "/f" : {
         "identity" : "LF",
         "description" : "Fire truck"
      },
      "/S" : {
         "identity" : "PS",
         "description" : "Space Shuttle"
      },
      "/c" : {
         "identity" : "LC",
         "description" : "Incident command post"
      },
      "\\s" : {
         "identity" : "SS",
         "description" : "Ship, boat"
      },
      "/1" : {
         "identity" : "P1",
         "description" : "Numbered circle: 1"
      },
      "\\f" : {
         "identity" : "SF",
         "description" : "Funnel cloud"
      },
      "\\=" : {
         "identity" : "NU",
         "unused" : 1
      },
      "/J" : {
         "identity" : "PJ",
         "unused" : 1
      },
      "/]" : {
         "identity" : "HU",
         "description" : "Mailbox, post office"
      },
      "\\G" : {
         "identity" : "AG",
         "description" : "Snow shower"
      },
      "/m" : {
         "identity" : "LM",
         "description" : "Mic-E repeater"
      },
      "\\!" : {
         "identity" : "OB",
         "description" : "Emergency"
      },
      "/C" : {
         "identity" : "PC",
         "description" : "Canoe"
      },
      "\\x" : {
         "identity" : "SX",
         "unused" : 1
      },
      "\\;" : {
         "identity" : "NS",
         "description" : "Park, picnic area"
      },
      "\\o" : {
         "identity" : "SO",
         "description" : "Small circle"
      },
      "\\$" : {
         "identity" : "OE",
         "description" : "Bank or ATM"
      },
      "/3" : {
         "identity" : "P3",
         "description" : "Numbered circle: 3"
      },
      "/+" : {
         "identity" : "BL",
         "description" : "Red Cross"
      },
      "\\l" : {
         "identity" : "SL",
         "unused" : 1
      },
      "/5" : {
         "identity" : "P5",
         "description" : "Numbered circle: 5"
      },
      "\\L" : {
         "identity" : "AL",
         "description" : "Lighthouse"
      },
      "\\\"" : {
         "identity" : "OC",
         "unused" : 1
      },
      "/a" : {
         "identity" : "LA",
         "description" : "Ambulance"
      },
      "/$" : {
         "identity" : "BE",
         "description" : "Telephone"
      },
      "\\:" : {
         "identity" : "NR",
         "description" : "Hail"
      },
      "/P" : {
         "identity" : "PP",
         "description" : "Police car"
      },
      "\\D" : {
         "identity" : "AD",
         "description" : "Drizzling rain"
      },
      "/N" : {
         "identity" : "PN",
         "description" : "NTS station"
      },
      "/e" : {
         "identity" : "LE",
         "description" : "Horse, equestrian"
      },
      "\\N" : {
         "identity" : "AN",
         "description" : "Navigation buoy"
      },
      "\\{" : {
         "identity" : "Q1",
         "description" : "Fog"
      },
      "\\q" : {
         "identity" : "SQ",
         "unused" : 1
      },
      "/F" : {
         "identity" : "PF",
         "description" : "Farm vehicle, tractor"
      },
      "\\Z" : {
         "identity" : "AZ",
         "unused" : 1
      },
      "\\M" : {
         "identity" : "AM",
         "unused" : 1
      },
      "/[" : {
         "identity" : "HS",
         "description" : "Human"
      },
      "/L" : {
         "identity" : "PL",
         "description" : "PC user"
      },
      "/(" : {
         "identity" : "BI",
         "description" : "Mobile satellite station"
      },
      "/w" : {
         "identity" : "LW",
         "description" : "Water station"
      },
      "\\[" : {
         "identity" : "DS",
         "description" : "Wall Cloud"
      },
      "/O" : {
         "identity" : "PO",
         "description" : "Balloon"
      },
      "\\d" : {
         "identity" : "SD",
         "description" : "DX spot"
      },
      "/2" : {
         "identity" : "P2",
         "description" : "Numbered circle: 2"
      },
      "/<" : {
         "identity" : "MT",
         "description" : "Motorcycle"
      },
      "\\?" : {
         "identity" : "NW",
         "description" : "Info kiosk"
      },
      "\\m" : {
         "identity" : "SM",
         "description" : "Value sign, 3 digit display"
      },
      "\\z" : {
         "identity" : "SZ",
         "description" : "Shelter"
      },
      "/%" : {
         "identity" : "BF",
         "description" : "DX cluster"
      },
      "/q" : {
         "identity" : "LQ",
         "description" : "Grid square, 2 by 2"
      },
      "/d" : {
         "identity" : "LD",
         "description" : "Fire station"
      },
      "/t" : {
         "identity" : "LT",
         "description" : "Truck stop"
      },
      "\\y" : {
         "identity" : "SY",
         "description" : "Skywarn"
      },
      "\\}" : {
         "identity" : "Q3",
         "unused" : 1
      },
      "/z" : {
         "identity" : "LZ",
         "description" : "Shelter"
      },
      "\\6" : {
         "identity" : "A6",
         "unused" : 1
      },
      "\\J" : {
         "identity" : "AJ",
         "description" : "Lightning"
      },
      "/6" : {
         "identity" : "P6",
         "description" : "Numbered circle: 6"
      },
      "\\," : {
         "identity" : "OM",
         "description" : "Girl Scouts"
      },
      "\\`" : {
         "identity" : "DX",
         "description" : "Rain"
      },
      "/4" : {
         "identity" : "P4",
         "description" : "Numbered circle: 4"
      },
      "\\H" : {
         "identity" : "AH",
         "description" : "Haze"
      },
      "/y" : {
         "identity" : "LY",
         "description" : "House, yagi antenna"
      },
      "\\i" : {
         "identity" : "SI",
         "description" : "Black box, point of interest"
      },
      "/!" : {
         "identity" : "BB",
         "description" : "Police station"
      },
      "/o" : {
         "identity" : "LO",
         "description" : "Emergency operations center"
      },
      "\\T" : {
         "identity" : "AT",
         "description" : "Thunderstorm"
      },
      "\\3" : {
         "identity" : "A3",
         "unused" : 1
      },
      "\\/" : {
         "identity" : "OP",
         "description" : "Waypoint destination"
      },
      "/l" : {
         "identity" : "LL",
         "description" : "Laptop"
      },
      "/_" : {
         "identity" : "HW",
         "description" : "Weather station"
      },
      "/>" : {
         "identity" : "MV",
         "description" : "Car"
      }
};

