#include<bits/stdc++.h>

#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

struct table{
	vector<string> columns;
	vector<vector<int> >records;
	int this_inst;
};

struct col{
	string table;
	string name;
};

map<string, table> tbl_data;
void readData(string tbl_name){
	string file_name = tbl_name + ".csv";
	ifstream tbl_file(file_name);
	string line;
	while (getline(tbl_file, line)){
		stringstream ss(line);
		vector<int> record;
		while(ss.good()){
			string n;
			getline(ss, n, ',');
			int val;
			if(n[0] == '\"'){
				n = n.substr(1,n.size()-2);
			}
			val = atoi(n.c_str());
			//cout << n <<   " hello" << val << endl;
			record.push_back(val);
		}
		tbl_data[tbl_name].records.push_back(record);
		tbl_data[tbl_name].this_inst = 1;
	}
}

int validate_column(Expr* expr){
	string col_name = expr->name;
	if(expr->table != NULL){
		string tableName = expr->table;
		if(tbl_data.find(tableName) == tbl_data.end() || tbl_data[tableName].this_inst == 0)
			return -1;
		else{
			table t = tbl_data[tableName];
			vector<string> columns = t.columns;
			if(find(columns.begin(),columns.end(),col_name)==columns.end())
				return -1;
			else 
				return 0;
		}
	}
	else{
		int table_presence = 0; //no of table this column is present in
		string finalName;
		for(map<string,table>::iterator it=tbl_data.begin();it!=tbl_data.end();it++){
			table t = tbl_data[it->first];
			vector<string> columns = t.columns;
			if(find(columns.begin(),columns.end(),col_name)!=columns.end() && t.this_inst == 1){
				table_presence++;
				finalName = it->first;
			}
		}
		if(table_presence != 1)
			return -1;
		else{
			expr->table = (char*)malloc(sizeof(finalName));
			strcpy(expr->table,(char*)finalName.c_str());
			return 0;
		}
	}
}

//hack for handling more distincts
vector<string> distinct_fields;
void replaceAll(std::string& str, const std::string& from, const std::string& to) {
	if(from.empty())
		return;
	size_t start_pos = 0;
	while((start_pos = str.find(from, start_pos)) != std::string::npos) {
		str.replace(start_pos, from.length(), to);
		start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
		int temp = start_pos;
		while(str[temp] == '(') temp++;
		string field;
		while(str[temp] != ')') field.push_back(str[temp]), temp++;
		distinct_fields.push_back(field);
	}
}

bool checkWhereCondition(vector<int>& record, vector<int>& result, vector<col> tbl_structure, Expr* where_clause, string table_name) {
	string left_operand_table = "";
	if (where_clause->expr->table != NULL)
		left_operand_table = string(where_clause->expr->table);
	string left_operand_name = "";
	if(where_clause->expr->name != NULL)
		left_operand_name = string(where_clause->expr->name);
	string right_operand_table = "";
	if(where_clause->expr2->table!=NULL)
		right_operand_table = string(where_clause->expr2->table);
	string right_operand_name = "";
	if(where_clause->expr2->name != NULL)
		right_operand_name = string(where_clause->expr2->name);

	int second_operand, first_operand;
	if(left_operand_table==table_name){
		if (where_clause->expr2->op_type == 9)
			second_operand = -1*where_clause->expr2->expr->ival;

		switch(where_clause->expr2->type){
			case kExprLiteralInt:
				second_operand = where_clause->expr2->ival;
				break;
			case kExprColumnRef:
				if(right_operand_table == table_name){
					int i =0, found = 0;
					for(string column: tbl_data[table_name].columns){
						if(right_operand_name==column){
							second_operand = record[i];
							found = 1;
							break;
						}
						i++;
					}
					if(found == 1)
						break;
				}
				int i =1, found = 0;
				for(col column: tbl_structure){
					if(column.table == right_operand_table && column.name == right_operand_name){
						second_operand = result[i];
						found = 1;
					}
					i++;
				}
				if(found == 0)
					return true;
				break;
		}
		int i =0;
		for(string column: tbl_data[table_name].columns){
			if(left_operand_name==column){
				first_operand = record[i];
				break;
			}
			i++;
		}
	} else if (right_operand_table ==table_name){
		if (where_clause->expr2->op_type == 9)
			first_operand = -1*where_clause->expr->expr->ival;
		switch(where_clause->expr->type){
			case kExprLiteralInt:
				first_operand = where_clause->expr->ival;
				break;
			case kExprColumnRef:
				int i =1, found = 0;
				for(col column: tbl_structure){
					if(column.table == left_operand_table && column.name == left_operand_name){
						first_operand = result[i];
						found = 1;
					}
					i++;
				}
				if(found == 0)
					return true;
				break;
		}
		int i =0;
		for(string column: tbl_data[table_name].columns){
			if(right_operand_name==column){
				second_operand = record[i];
				break;
			}
			i++;
		}
	}
	else if(where_clause->expr->type == kExprLiteralInt && where_clause->expr2->type == kExprLiteralInt)
		first_operand = where_clause->expr->ival, second_operand = where_clause->expr2->ival;
	else
		return true;

	if(where_clause->op_type == Expr::SIMPLE_OP){
		switch(where_clause->op_char){
			case '=':
				return (first_operand == second_operand);
			case '>':
				return (first_operand > second_operand);
			case '<':
				return (first_operand < second_operand);
		}
	} else {
		switch(where_clause->op_type){
			case Expr::NOT_EQUALS:
				return (first_operand != second_operand);
			case Expr::LESS_EQ:
				return (first_operand <= second_operand);
			case Expr::GREATER_EQ:
				return (first_operand >= second_operand);
		}
	}
	return true;
}

int main(int argc, char *argv[]) {
	try {
		string line;
		ifstream meta_file("metadata.txt");
		if (meta_file.is_open()) {
			while (getline(meta_file, line)) {
				line = line.substr(0,line.size()-1);
				if(line.find("<begin_table>") == 0){
					getline(meta_file, line);
					line = line.substr(0,line.size()-1);
					string table_name = line;
					table t;
					while(getline(meta_file, line)){
						line = line.substr(0,line.size()-1);
						if(line.find("<end_table")==0)
							break;
						//cout<<line;
						t.columns.push_back(line);
					}
					t.this_inst = 0;
					tbl_data[table_name] = t;
				}
			}
		}
		if(argc < 2){
			cout<<"Query is missing"<<endl;
			cout<<"Example use: ./engine \"SELECT * from table1;\""<<endl;
			return 0;
		}
		meta_file.close();
		string query;
		query = argv[1];
		replaceAll(query, "distinct", "");
		SQLParserResult* result = SQLParser::parseSQLString(query);
		vector<string> tables;
		if (result->isValid) {
			Expr* where_clause = NULL;
			vector<Expr*> *field_list = NULL;
			for (SQLStatement* stmt : result->statements) {
				/* printStatementInfo(stmt); */
				switch(stmt->type()){
					case kStmtSelect:{
								 SelectStatement* stmt_sel = (SelectStatement*)stmt;
								 TableRef* table = stmt_sel->fromTable;
								 if(stmt_sel->whereClause != NULL)
									 where_clause = stmt_sel->whereClause;
								 field_list = stmt_sel->selectList;
								 switch(table->type) {
									 case kTableName:
										 readData(table->name);
										 tables.push_back(table->name);
										 break;
									 case kTableCrossProduct:
										 for(TableRef* tbl: *table->list) {
											 readData(tbl->name);
											 tables.push_back(tbl->name);
										 }
										 break;
									 default:
										 cout<<"Not handled right now";
										 return -1;
										 break;
								 }
							 }
							 break;
					default:
							 cout<<"Invalid or not handled currently";
							 return -1;
							 break;
				};
			}

			//Adding distinct flag (part of the hack)
			for (Expr* field: *field_list){
				if (find (distinct_fields.begin(), distinct_fields.end(),field->name)!=distinct_fields.end())
					field->distinct = true;
			}

			//Validating
			int join = 0;
			vector<col> join_col;
			col temp_col;
			int result;
			if (where_clause != NULL){		
				switch(where_clause->op_type){
					case Expr::SIMPLE_OP:
					case Expr::NOT_EQUALS:
					case Expr::LESS_EQ:
					case Expr::GREATER_EQ:
						if(where_clause->expr != NULL && where_clause->expr->type == kExprColumnRef)
							result = validate_column(where_clause->expr), join++;
						if(where_clause->expr2 != NULL && where_clause->expr2->type == kExprColumnRef)
							result = min(validate_column(where_clause->expr2),result), join++;
						if(join == 2 && where_clause->op_type == Expr::SIMPLE_OP && where_clause->op_char == '='){
							temp_col.table = string(where_clause->expr->table);
							temp_col.name = string(where_clause->expr->name);
							join_col.push_back(temp_col);
						}
						break;
					case Expr::AND:
					case Expr::OR:
						if(where_clause->expr->expr != NULL && where_clause->expr->expr->type == kExprColumnRef)
							result = min(validate_column(where_clause->expr->expr),result), join++;
						if(where_clause->expr->expr2 != NULL && where_clause->expr->expr2->type == kExprColumnRef)
							result = min(validate_column(where_clause->expr->expr2),result), join++;
						if(join == 2 && where_clause->op_type == Expr::AND){
							temp_col.table = string(where_clause->expr->expr->table), temp_col.name = string(where_clause->expr->expr->name);
							join_col.push_back(temp_col);
						}
						join = 0;
						if(where_clause->expr2->expr != NULL && where_clause->expr2->expr->type == kExprColumnRef)
							result = min(validate_column(where_clause->expr2->expr),result), join++;
						if(where_clause->expr2->expr2 != NULL && where_clause->expr2->expr2->type == kExprColumnRef)
							result = min(validate_column(where_clause->expr2->expr2), result), join++;
						if(join == 2 && where_clause->op_type == Expr::AND){
							temp_col.table = string(where_clause->expr->expr->table), temp_col.name = string(where_clause->expr->expr->name);
							join_col.push_back(temp_col);
						}
						break;
					default:
						cout<<"Not handled right now pp";
						return -1;
						break;
				}
			}

			int agg = 0, nonagg = 0, all = 0;
			for (Expr* field: *field_list){
				switch(field->type){
					case kExprColumnRef:
						result = min(validate_column(field),0);
						nonagg = 1;
						break;
					case kExprFunctionRef:
						result = min(validate_column(field->expr),0);
						agg = 1;
						break;
					case kExprStar:
						all = 1;
						break;
				}
			}
			if (agg&nonagg || agg&all){ //Aggregate and non-aggregate functions cannot be used at same time
				cout<<"Aggregate and non aggregate functions cannot be used at once"<<endl;
				return -1;	//Similarly, * and non-aggregate functions also cannot be used
			}
			if (result == -1){
				cout<<"Ambiguity in where clause or fields to be selected"<<endl;
				return -1;
			}


			//Retrieving results

			vector<vector<int> > results_new, results;
			vector<col> tbl_structure;
			map<string, int> added_tables;
			vector<int> temp;
			temp.push_back(0);
			results.push_back(temp);
			for (string tbl: tables){
				/* cout<<tbl<<endl; */
				table t = tbl_data[tbl];
				for(vector<int> record: t.records){
					for(vector<int> result: results){
						bool accepted = false;
						bool res, res1, res2; 
						if(where_clause!=NULL){
							switch(where_clause->op_type){
								case Expr::SIMPLE_OP:
								case Expr::NOT_EQUALS:
								case Expr::LESS_EQ:
								case Expr::GREATER_EQ:
									res = checkWhereCondition(record, result, tbl_structure, where_clause, tbl);
									accepted = res;
									break;
								case Expr::AND:
									res1 = checkWhereCondition(record, result, tbl_structure, where_clause->expr, tbl);
									res2 = checkWhereCondition(record, result, tbl_structure, where_clause->expr2, tbl);
									accepted = res1 & res2;
									break;
								case Expr::OR:
									res1 = checkWhereCondition(record, result, tbl_structure, where_clause->expr, tbl);
									res2 = checkWhereCondition(record, result, tbl_structure, where_clause->expr2, tbl);
									accepted = res1 | res2;
									break;
								default:
									cout<<"Not handled right now";
									return -1;
									break;
							}
						}
						else
							accepted = true;
						if(accepted){
							vector<int> new_result = result;
							new_result.insert(new_result.end(), record.begin(), record.end());
							results_new.push_back(new_result);
						}
					}
				}
				results.clear();
				results = results_new;
				results_new.clear();
				/* cout<<results.size(); */
				for (vector<int> result: results){
					for (int val: result){
						/* cout<<val<<" "; */
					}
					/* cout<<endl; */
				}
				for (string column: t.columns){
					col new_col;
					new_col.table = tbl;
					new_col.name = column;
					tbl_structure.push_back(new_col);
				}
			}
			for (vector<int> result: results){
				for (int val: result){
					/* cout<<val<<" "; */
				}
				/* cout<<endl; */
			}

			//Apply projection, aggregate functions and distinct
			vector<vector<int> > finalResults;
			int column_index;
			vector<int> new_column, temp_column;
			vector<col> finalTblStructure;
			string aggFunctionName;
			for(Expr* field: *field_list){
				switch(field->type){
					case kExprColumnRef:
						column_index = 1;
						for(col column: tbl_structure){
							if(field->table == column.table && field->name == column.name){
								finalTblStructure.push_back(column);
								break;
							}
							column_index++;
						}
						for(vector<int> result: results){
							new_column.push_back(result[column_index]);
						}
						if(field->distinct){
							sort(new_column.begin(), new_column.end());
							vector<int>::iterator it;
							it = unique (new_column.begin(), new_column.end());
							new_column.resize(distance(new_column.begin(),it) );
						} 
						finalResults.push_back(new_column);
						new_column.clear();
						break;
					case kExprFunctionRef:
						column_index=1;
						for(col column: tbl_structure){
							if(field->expr->table == column.table && field->expr->name == column.name){
								break;
							}
							column_index++;
						}
						for(vector<int> result: results){
							temp_column.push_back(result[column_index]);
						}
						aggFunctionName = string(field->name);
						temp_col = tbl_structure[column_index-1];
						if(aggFunctionName == "max"){
							new_column.push_back(*max_element(temp_column.begin(), temp_column.end()));
							temp_col.name = "max(" + temp_col.name + ")";
							finalTblStructure.push_back(temp_col);
						}else if(aggFunctionName == "min"){
							new_column.push_back(*min_element(temp_column.begin(), temp_column.end()));
							temp_col.name = "min(" + temp_col.name + ")";
							finalTblStructure.push_back(temp_col);
						}else if (aggFunctionName == "sum" || aggFunctionName == "avg"){
							int sum = 0;
							for(int val: temp_column)
								sum+=val;
							if(aggFunctionName == "sum"){
								temp_col.name = "sum(" + temp_col.name + ")";
								finalTblStructure.push_back(temp_col);
								new_column.push_back(sum);
							}
							else{
								temp_col.name = "avg(" + temp_col.name + ")";
								finalTblStructure.push_back(temp_col);
								new_column.push_back(sum/temp_column.size());
							}
						}
						finalResults.push_back(new_column);
						new_column.clear();
						break;
					case kExprStar:
						column_index = 1;
						for(col column: tbl_structure){
							int dontAdd = 0;
							for(col column_j: join_col)
								if (column.name == column_j.name && column.table == column_j.table)
									dontAdd = 1;
							if(dontAdd == 1)
								continue;
							for(vector<int> result: results){
								new_column.push_back(result[column_index]);
							}
							finalTblStructure.push_back(column);
							finalResults.push_back(new_column);
							new_column.clear();
							column_index++;
						}
						break;
					default:
						cout<<"Something is wrong";
						return -1;
						break;
				}
			}
			for(col column: finalTblStructure)
				cout<<column.table<<"."<<column.name<<"\t";
			cout<<endl;
			int max_size = 0;
			for(vector<int> v:finalResults)
				max_size = max(max_size, (int)v.size());
			for(int i = 0;i<max_size;i++){
				for(vector<int> v:finalResults){
					if(i<v.size())
						cout<<v[i]<<"\t";
					else
						cout<<"\t";
				}
				cout<<endl;
			}
		}
		else {
			cout<<"Invalid query";
		}
	}
	catch (int e){
		cout<<"sorry some error occured"<<endl;
	}
	return 0;
}
