grammar JsonPath;

@header {
package io.github.rerorero.kafka.jsonpath.parser;
// Do not edit this file directly. This file is auto generated by antlr4.
}

ROOT : '$' ;
WILDCARD : '*' ;
BRACKET_LEFT : '[' ;
BRACKET_RIGHT : ']' ;
SUBSCRIPT_DOT : '.' ;

ID
  : [_A-Za-z] [_A-Za-z0-9]*
  ;

STRING
  : '\'' SAFECODEPOINT* '\''
  ;

fragment SAFECODEPOINT
  : ~ ['\\\u0000-\u001F]
  ;

NUMBER
  : '-'? INT
  ;

fragment INT
  : '0' | [1-9] [0-9]*
  ;

WS
  : [ \t\n\r] + -> skip
  ;

jsonpath
  : ROOT subscript* EOF
  ;

subscript
  : subscriptDot
  | subscriptBracket
  ;

subscriptDot
  : SUBSCRIPT_DOT ID arraySub?
  ;

subscriptBracket
  : BRACKET_LEFT STRING BRACKET_RIGHT arraySub?
  ;

arraySub
  : BRACKET_LEFT (NUMBER|WILDCARD) BRACKET_RIGHT
  ;
