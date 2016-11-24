CFLAGS = -std=c++11 -lstdc++ -w -g -IInclude/ -Llib/
CXX = g++-5
all:
	$(CXX) $(CFLAGS) engine.cpp -o engine -lsqlparser

test:
	./engine "SELECT * from table1;"
	./engine "SELECT max(A) from table1;"
	./engine "SELECT min(B) from table1;"
	./engine "SELECT avg(C) from table1;"
	./engine "SELECT sum(D) from table2;"
	./engine "SELECT A from table1;"
	./engine "SELECT A,D from table1, table2;"
	./engine "SELECT distinct(C) from table3;"
	./engine "SELECT B,C from table1 where A=-900;"
	./engine "SELECT A,B from table1 where A=775 or B=803;"
	./engine "SELECT A,B from table1 where A=922 or B=158;"
	./engine "SELECT * from table1, table2 where table1.B=table2.B;"
	./engine "SELECT A,D from table1, table2 where table1.B=table2.B;"

clean:
	rm engine
