/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
grammar DatabricksSqlBase;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

statement
    : managePermissions                                                 #managePermissionsAlt
    | ALTER securable OWNER TO identifier                               #alterOwner
    | MSCK REPAIR securable PRIVILEGES                                  #repairPrivileges
    | SHOW GRANT identifier? ON (ALL| securable)                        #showPermissions
    | VACUUM (path=STRING | table=qualifiedName) (RETAIN number HOURS)? #vacuumTable
    | .*?                                                               #passThrough
    ;

managePermissions
    : GRANT (actionTypes+=identifier (',' actionTypes+=identifier)* | ALL PRIVILEGES)
      ON securable TO grantee=identifier
      (WITH GRANT OPTION)?
    | REVOKE (GRANT OPTION FOR)?
      (actionTypes+=identifier (',' actionTypes+=identifier)* | ALL PRIVILEGES)
      ON securable FROM grantee=identifier
    ;

securable
    : objectType=CATALOG
    | objectType=DATABASE identifier
    | objectType=VIEW qualifiedName
    | objectType=FUNCTION qualifiedName
    | ANONYMOUS objectType=FUNCTION
    | ANY objectType=FILE
    | objectType=TABLE? qualifiedName
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : DECIMAL_VALUE            #decimalLiteral
    | INTEGER_VALUE            #integerLiteral
    ;

nonReserved
    : ALTER | OWNER | TO | MSCK | REPAIR | PRIVILEGES | SHOW | GRANT | ON | ALL | WITH | OPTION
    | REVOKE | FOR | FROM | CATALOG | DATABASE | TABLE | VIEW | FUNCTION | ANONYMOUS | FILE | ANY
    | VACUUM | RETAIN | HOURS
    ;

ALTER: 'ALTER';
OWNER: 'OWNER';
TO: 'TO';
MSCK: 'MSCK';
REPAIR: 'REPAIR';
PRIVILEGES: 'PRIVILEGES';
SHOW: 'SHOW';
GRANT: 'GRANT';
ON: 'ON';
ALL: 'ALL';
WITH: 'WITH';
OPTION: 'OPTION';
REVOKE: 'REVOKE';
FOR: 'FOR';
FROM: 'FROM';
CATALOG: 'CATALOG';
DATABASE: 'DATABASE';
TABLE: 'TABLE';
VIEW: 'VIEW';
FUNCTION: 'FUNCTION';
ANONYMOUS: 'ANONYMOUS';
FILE: 'FILE';
ANY: 'ANY';
VACUUM: 'VACUUM';
RETAIN: 'RETAIN';
HOURS: 'HOURS';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
