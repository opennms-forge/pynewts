import datetime

from enum import Enum

def round_down(num, divisor):
    return num - (num%divisor)

def df(ts):
    return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.000+0000')

class ValueType(Enum):
  COUNTER = 1
  ABSOLUTE = 2
  DERIVE = 3
  GAUGE = 4

def decompose(bytes):
    # This is NOT complete - see https://github.com/OpenNMS/newts/blob/1.5.0/api/src/main/java/org/opennms/newts/api/ValueType.java#L103
    type = ValueType(bytes[0])
    value_bytes = bytes[1:]
    value = int.from_bytes(value_bytes, byteorder='big')
    return (type, value)
