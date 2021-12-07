#!/usr/bin/env python3

import os
import json
import re

class Generator:
  def __init__(self):
    self._nodetypes = json.load(open('./srcdata/nodetypes.json'))
    self._struct_defs = json.load(open('./srcdata/struct_defs.json'))
    self._enum_defs = json.load(open('./srcdata/enum_defs.json'))
    self._typedefs = json.load(open('./srcdata/typedefs.json'))

  def underscore(self, camel_cased_word):
    if re.search(r'[A-Z-]|::', camel_cased_word) == None:
      return camel_cased_word
    word = camel_cased_word.replace("::", "/")
    word = re.sub(r'^([A-Z\d])([A-Z][a-z])', r'\1__\2', word)
    word = re.sub(r'([A-Z\d]+[a-z]+)([A-Z][a-z])', r'\1_\2', word)
    word = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', word)
    word.replace("-", "_")
    return word.lower()

  def generate_outmethods(self):
    self._outmethods = {}
    self._readmethods = {}
    self._protobuf_messages = {}
    self._protobuf_enums = {}
    self._scan_protobuf_tokens = []
    self._enum_to_strings = {}
    self._enum_to_ints = {}
    self._int_to_enums = {}

    for group in ['nodes/parsenodes', 'nodes/primnodes']:
      for node_type in self._struct_defs[group]:
        self._outmethods[node_type] = ''
        self._readmethods[node_type] = ''
        self._protobuf_messages[node_type] = ''
        protobuf_field_count = 1

        for field_def in self._struct_defs[group][node_type]['fields']:
          if not 'name' in field_def:
            continue
          if not 'c_type' in field_def:
            continue

          name = field_def['name']
          orig_type = field_def['c_type']

          # TODO: Add comments to protobuf definitions

          if node_type == 'Query' and name == 'queryId':
            type = ':skip'
          else:
            type = orig_type
          if node_type == 'CreateForeignTableStmt' and name == 'base':
            outname = 'base_stmt'
          else:
            outname = self.underscore(name)
          outname_json = name

          if type == ':skip':
            pass # Ignore
          elif type == 'NodeTag':
            pass # Nothing
          elif type in ['char']:
            self._outmethods[node_type] += "  WRITE_CHAR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_CHAR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  string {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['bool']:
            self._outmethods[node_type] += "  WRITE_BOOL_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_BOOL_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  bool {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['long']:
            self._outmethods[node_type] += "  WRITE_LONG_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_LONG_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  int64 {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['int', 'int16', 'int32', 'AttrNumber']:
            self._outmethods[node_type] += "  WRITE_INT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_INT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  int32 {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['uint', 'uint16', 'uint32', 'Index', 'bits32', 'Oid', 'AclMode', 'SubTransactionId']:
            self._outmethods[node_type] += "  WRITE_UINT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_UINT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  uint32 {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type == 'char*':
            self._outmethods[node_type] += "  WRITE_STRING_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_STRING_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  string {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['float', 'double', 'Cost', 'Selectivity']:
            self._outmethods[node_type] += "  WRITE_FLOAT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_FLOAT_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  double {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Bitmapset*', 'Relids']:
            self._outmethods[node_type] += "  WRITE_BITMAPSET_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_BITMAPSET_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  repeated uint64 {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Value']:
            self._outmethods[node_type] += "  WRITE_NODE_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_VALUE_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Value*']:
            self._outmethods[node_type] += "  WRITE_NODE_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_VALUE_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['List*']:
            self._outmethods[node_type] += "  WRITE_LIST_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_LIST_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  repeated Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Node*']:
            self._outmethods[node_type] += "  WRITE_NODE_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_NODE_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Expr*']:
            self._outmethods[node_type] += "  WRITE_NODE_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._readmethods[node_type] += "  READ_EXPR_PTR_FIELD({}, {}, {});\n".format(outname, outname_json, name)
            self._protobuf_messages[node_type] += "  Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['Expr']:
            # FIXME
            self._protobuf_messages[node_type] += "  Node {} = {} [json_name=\"{}\"];\n".format(outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type in ['CreateStmt']:
            self._outmethods[node_type] += "  WRITE_SPECIFIC_NODE_FIELD({}, {}, {}, {}, {});\n".format(type.replace('*', ''), self.underscore(type.replace('*', '')).lower(), outname, outname_json, name)
            self._readmethods[node_type] += "  READ_SPECIFIC_NODE_FIELD({}, {}, {}, {}, {});\n".format(type.replace('*', ''), self.underscore(type.replace('*', '')).lower(), outname, outname_json, name)
            self._protobuf_messages[node_type] += "  {} {} = {} [json_name=\"{}\"];\n".format(type.replace('*', ''), outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif re.sub('\*', '', type) in self._nodetypes:
            self._outmethods[node_type] += "  WRITE_SPECIFIC_NODE_PTR_FIELD({}, {}, {}, {}, {});\n".format(type.replace('*', ''), self.underscore(type.replace('*', '')).lower(), outname, outname_json, name)
            self._readmethods[node_type] += "  READ_SPECIFIC_NODE_PTR_FIELD({}, {}, {}, {}, {});\n".format(type.replace('*', ''), self.underscore(type.replace('*', '')).lower(), outname, outname_json, name)
            self._protobuf_messages[node_type] += "  {} {} = {} [json_name=\"{}\"];\n".format(type.replace('*', ''), outname, protobuf_field_count, name)
            protobuf_field_count += 1
          elif type.endswith('*'):
            print(f'ERR: {name:s} {type:s}')
          else: # Enum
            self._outmethods[node_type] += "  WRITE_ENUM_FIELD({}, {}, {}, {});\n".format(type, outname, outname_json, name)
            self._readmethods[node_type] += "  READ_ENUM_FIELD({}, {}, {}, {});\n".format(type, outname, outname_json, name)
            self._protobuf_messages[node_type] += "  {} {} = {} [json_name=\"{}\"];\n".format(type, outname, protobuf_field_count, name)
            protobuf_field_count += 1

    for group in ['nodes/parsenodes', 'nodes/primnodes', 'nodes/nodes', 'nodes/lockoptions']:
      for enum_type in self._enum_defs[group]:
        enum_def = self._enum_defs[group][enum_type]
        if enum_type == 'NodeTag':
          continue

        self._protobuf_enums[enum_type] = "enum {}\n{{\n".format(enum_type)
        self._enum_to_strings[enum_type] = "static const char*\n_enumToString{}({} value) {{\n  switch(value) {{\n".format(enum_type, enum_type)
        self._enum_to_ints[enum_type] = "static int\n_enumToInt{}({} value) {{\n  switch(value) {{\n".format(enum_type, enum_type)
        self._int_to_enums[enum_type] = "static {}\n_intToEnum{}(int value) {{\n  switch(value) {{\n".format(enum_type, enum_type)

        # We intentionally add a dummy field for the zero value, that actually is not used in practice
        # - this ensures that the JSON output always includes the enum value (and doesn't skip it because its the zero value)
        self._protobuf_enums[enum_type] += "  {}_UNDEFINED = 0;\n".format(self.underscore(enum_type).upper())
        protobuf_field = 1

        for value in enum_def['values']:
          if not 'name' in value:
            continue

          self._protobuf_enums[enum_type] += "  {} = {};\n".format(value['name'], protobuf_field)
          self._enum_to_strings[enum_type] += "    case {}: return \"{}\";\n".format(value['name'], value['name'])
          self._enum_to_ints[enum_type] += "    case {}: return {};\n".format(value['name'], protobuf_field)
          self._int_to_enums[enum_type] += "    case {}: return {};\n".format(protobuf_field, value['name'])
          protobuf_field += 1

        self._protobuf_enums[enum_type] += "}"
        self._enum_to_strings[enum_type] += "  }\n  Assert(false);\n  return NULL;\n}"
        self._enum_to_ints[enum_type] += "  }\n  Assert(false);\n  return -1;\n}"
        for v in enum_def['values']:
          if not 'name' in v:
            continue
          self._int_to_enums[enum_type] += "  }}\n  Assert(false);\n  return {};\n}}".format(v['name'])
          break

    scan_values = self._enum_defs['../backend/parser/gram']['yytokentype']['values']
    for value in scan_values:
      if not value['name']:
        continue
      self._scan_protobuf_tokens.append('{} = {};'.format(value['name'], value['value']))

    for typedef in self._typedefs:
      if not 'source_type' in typedef:
        continue
      if not typedef['source_type'] in self._outmethods:
        continue

      self._outmethods[typedef['new_type_name']] = self._outmethods[typedef['source_type']]
      self._readmethods[typedef['new_type_name']] = self._readmethods[typedef['source_type']]
      self._protobuf_messages[typedef['new_type_name']] = self._protobuf_messages[typedef['source_type']]

  IGNORE_LIST = [
    'Value', # Special case
    'Const', # Only needed in post-parse analysis (and it introduces Datums, which we can't output)
  ]
  EXPLICT_TAG_SETS = [ # These nodes need an explicit NodeSetTag during read funcs because they are a superset of another node
    'CreateForeignTableStmt',
  ]

  def generate(self):
    self.generate_outmethods()

    out_defs = ''
    out_impls = ''
    out_conds = """case T_Integer:
  OUT_NODE(Integer, Integer, integer, INTEGER, Value, integer);
  break;
case T_Float:
  OUT_NODE(Float, Float, float, FLOAT, Value, float_);
  break;
case T_String:
  OUT_NODE(String, String, string, STRING, Value, string);
  break;
case T_BitString:
  OUT_NODE(BitString, BitString, bit_string, BIT_STRING, Value, bit_string);
  break;
case T_Null:
  OUT_NODE(Null, Null, null, NULL, Value, null);
  break;
case T_List:
  OUT_NODE(List, List, list, LIST, List, list);
  break;
case T_IntList:
  OUT_NODE(IntList, IntList, int_list, INT_LIST, List, int_list);
  break;
case T_OidList:
  OUT_NODE(OidList, OidList, oid_list, OID_LIST, List, oid_list);
  break;
"""
    read_defs = ''
    read_impls = ''
    read_conds = ''
    protobuf_messages = ''
    protobuf_nodes = []

    for type in self._nodetypes:
      if type in self.IGNORE_LIST:
        continue
      if not type in self._outmethods:
        continue
      if not type in self._readmethods:
        continue
      outmethod = self._outmethods[type]
      readmethod = self._readmethods[type]

      c_type = re.sub(r'_', '', type)

      out_defs += "static void _out{}(OUT_TYPE({}, {}) out_node, const {} *node);\n".format(c_type, type, c_type, type)

      out_impls += "static void\n"
      out_impls += "_out{}(OUT_TYPE({}, {}) out, const {} *node)\n".format(c_type, type, c_type, type)
      out_impls += "{\n"
      out_impls += outmethod
      out_impls += "}\n"
      out_impls += "\n"

      out_conds += "case T_{}:\n".format(type)
      out_conds += "  OUT_NODE({}, {}, {}, {}, {}, {});\n".format(type, c_type, self.underscore(c_type), self.underscore(c_type).upper().replace('__', '_'), type, self.underscore(type))
      out_conds += "  break;\n"

      read_defs += "static {} * _read{}(OUT_TYPE({}, {}) msg);\n".format(type, c_type, type, c_type)

      read_impls += "static {} *\n".format(type)
      read_impls += "_read{}(OUT_TYPE({}, {}) msg)\n".format(c_type, type, c_type)
      read_impls += "{\n"
      read_impls += "  {} *node = makeNode({});\n".format(type, type)
      read_impls += readmethod
      if type in self.EXPLICT_TAG_SETS:
        read_impls += "  NodeSetTag(node, T_{});\n".format(type)
      read_impls += "  return node;\n"
      read_impls += "}\n"
      read_impls += "\n"

      read_conds += "  READ_COND({}, {}, {}, {}, {}, {});\n".format(type, c_type, self.underscore(c_type), self.underscore(c_type).upper().replace('__', '_'), type, self.underscore(type))

      protobuf_messages += "message {}\n{{\n".format(type)
      protobuf_messages += self._protobuf_messages[type] or ''
      protobuf_messages += "}\n\n"

      protobuf_nodes.append("{} {} = {} [json_name=\"{}\"];".format(type, self.underscore(type), len(protobuf_nodes) + 1, type))

    for type in ['Integer', 'Float', 'String', 'BitString', 'Null', 'List', 'IntList', 'OidList']:
      protobuf_nodes.append("{} {} = {} [json_name=\"{}\"];".format(type, self.underscore(type), len(protobuf_nodes) + 1, type))

    protobuf_messages += "\n\n".join(self._protobuf_enums.values())

    with open('./src/pg_query_enum_defs.c', 'w') as f:
      f.write("// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py\n\n" +
        "\n\n".join(self._enum_to_strings.values()) + "\n\n".join(self._enum_to_ints.values()) + "\n\n".join(self._int_to_enums.values()))

    with open('./src/pg_query_outfuncs_defs.c', 'w') as f:
      f.write("// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py\n\n" +
        out_defs + "\n\n" + out_impls)

    with open('./src/pg_query_outfuncs_conds.c', 'w') as f:
      f.write("// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py\n\n" + out_conds)

    with open('./src/pg_query_readfuncs_defs.c', 'w') as f:
      f.write("// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py\n\n" +
        read_defs + "\n\n" + read_impls)

    with open('./src/pg_query_readfuncs_conds.c', 'w') as f:
      f.write("// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py\n\n" + read_conds)

    protobuf = """// This file is autogenerated by ./scripts/generate_protobuf_and_funcs.py

syntax = \"proto3\";

package pg_query;

message ParseResult {{
  int32 version = 1;
  repeated RawStmt stmts = 2;
}}

message ScanResult {{
  int32 version = 1;
  repeated ScanToken tokens = 2;
}}

message Node {{
  oneof node {{
		{}
  }}
}}

message Integer
{{
  int32 ival = 1; /* machine integer */
}}

message Float
{{
  string str = 1; /* string */
}}

message String
{{
  string str = 1; /* string */
}}

message BitString
{{
  string str = 1; /* string */
}}

message Null
{{
  // intentionally empty
}}

message List
{{
  repeated Node items = 1;
}}

message OidList
{{
  repeated Node items = 1;
}}

message IntList
{{
  repeated Node items = 1;
}}

{}

message ScanToken {{
  int32 start = 1;
  int32 end = 2;
  Token token = 4;
  KeywordKind keyword_kind = 5;
}}

enum KeywordKind {{
  NO_KEYWORD = 0;
  UNRESERVED_KEYWORD = 1;
  COL_NAME_KEYWORD = 2;
  TYPE_FUNC_NAME_KEYWORD = 3;
  RESERVED_KEYWORD = 4;
}}

enum Token {{
  NUL = 0;
  // Single-character tokens that are returned 1:1 (identical with \"self\" list in scan.l)
  // Either supporting syntax, or single-character operators (some can be both)
  // Also see https://www.postgresql.org/docs/12/sql-syntax-lexical.html#SQL-SYNTAX-SPECIAL-CHARS
  ASCII_37 = 37; // \"%\"
  ASCII_40 = 40; // \"(\"
  ASCII_41 = 41; // \")\"
  ASCII_42 = 42; // \"*\"
  ASCII_43 = 43; // \"+\"
  ASCII_44 = 44; // \",\"
  ASCII_45 = 45; // \"-\"
  ASCII_46 = 46; // \".\"
  ASCII_47 = 47; // \"/\"
  ASCII_58 = 58; // \":\"
  ASCII_59 = 59; // \";\"
  ASCII_60 = 60; // \"<\"
  ASCII_61 = 61; // \"=\"
  ASCII_62 = 62; // \">\"
  ASCII_63 = 63; // \"?\"
  ASCII_91 = 91; // \"[\"
  ASCII_92 = 92; // \"\\\"
  ASCII_93 = 93; // \"]\"
  ASCII_94 = 94; // \"^\"
  // Named tokens in scan.l
  {}
}}
""".format("\n    ".join(protobuf_nodes), protobuf_messages, "\n  ".join(self._scan_protobuf_tokens))

    with open('./protobuf/pg_query.proto', 'w') as f:
        f.write(protobuf)

gen = Generator()
gen.generate()