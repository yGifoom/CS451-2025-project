#ifndef TESTS_H
#define TESTS_H

#include "../include/parser.h"

void tests(char* res, Parser* parser);
void testLogger(char* res, Parser* parser);
void testUdp(char* res, Parser* parser);
void testPflx(char* res, Parser* parser);
void testQueue(char* res, Parser* parser);
void testNodeSeq(char* res, Parser* parser);
void testBstSet(char* res, Parser* parser);
void testBa(char* res, Parser* parser);
void testFifo(char* res, Parser* parser);
void testLA(char* res, Parser* parser);


#endif // TESTS_H

