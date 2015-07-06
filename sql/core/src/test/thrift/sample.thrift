namespace java org.apache.spark.sql.parquet.thrift

enum SampleEnum {
  VALUE_1 = 1,
  VALUE_2,
  value_3 = 0x0a
}

struct SampleObject {
  10: byte _byte;
  20: i16 _i16;
  30: i32 _i32;
  40: i64 _i64;
  50: bool _bool;
  60: double _double;
  60: binary _binary;
  70: string _string;
  80: list<i32> _i32_list;
  90: set<string> _string_set;
  100: map<i32, string> _i32_string_map;
  110: SampleEnum _enum;
}
