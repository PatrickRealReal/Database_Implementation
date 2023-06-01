
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
#include <unordered_map>
#include <sstream>

void replace_all2(string& s,string const& toReplace, string const& replaceWith) {
    std::ostringstream oss;
    std::size_t pos = 0;
    std::size_t prevPos = pos;

    while (true) {
        prevPos = pos;
        pos = s.find(toReplace, pos);
        if (pos == std::string::npos)
            break;
        oss << s.substr(prevPos, pos - prevPos);
        oss << replaceWith;
        pos += toReplace.size();
    }

    oss << s.substr(prevPos);
    s = oss.str();
}

//bool LessSort (pair<int, int> a,pair<int, int> b) {
//    return (a.second<b.second);
//}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// vector <ExprTreePtr> valuesToSelect;
//	vector <pair <string, string>> tablesToProcess;
//	vector <ExprTreePtr> allDisjunctions;
//	vector <ExprTreePtr> groupingClauses;
LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {

	// also, make sure that there are no aggregates in herre
	bool areAggs = false;
	for (auto a : valuesToSelect) {
		if (a->hasAgg ()) {
			areAggs = true;
            break;
		}
	}

    if (tablesToProcess.size () == 1){
        string tableName = tablesToProcess[0].first;
        string tableAlias = tablesToProcess[0].second;
        MyDB_TablePtr leftTable = allTables[tableName];
        vector <ExprTreePtr> leftCNF = allDisjunctions;

        MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
        vector <string> leftExprs;//用于select

        if(areAggs){
            //1st scan 的schema
            MyDB_SchemaPtr outputSchema = make_shared <MyDB_Schema> ();

            for (auto b: leftTable->getSchema ()->getAtts ()) {
                bool need_it = false;
                for(auto a : valuesToSelect){
                    if(a->referencesAtt(tableAlias, b.first)){
                        need_it = true;
                        break;
                    }
                }
                if(need_it){
                    //schema去掉了alias
                    outputSchema->getAtts ().push_back (make_pair (b.first, b.second));
                    leftExprs.push_back ("[" + b.first + "]");
//                    cout << "1st scan leftExprs: " << ("[" + b.first + "]") << "\n";
                }
            }

//            cout << "temp schema: " << outputSchema << "\n";
            LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tableName],
                                                                         make_shared <MyDB_Table> ("interLeftTable", "interLeftStorageLoc", outputSchema),
                                                                         make_shared <MyDB_Stats> (leftTable, tableAlias), leftCNF, leftExprs, tableAlias);

            //准备aggregate
            MyDB_Record myRec (leftTable->getSchema());

            //两个schema
            MyDB_SchemaPtr agg_Schema = make_shared <MyDB_Schema> ();
            MyDB_SchemaPtr top_Schema = make_shared <MyDB_Schema> ();//2nd scan
            //for agg_schema
            vector<string> attributes;
            vector<string> aggregates;

            vector<string> value_order;
            vector<string> top_projection;
            // 记录 att index的 hashmap
            unordered_map<string, string> att_index;

            for(auto a:valuesToSelect){
                //没有替换
                string value_select_limit = a->toString ();
//                cout<<"aggregate - valuesToSelect: "<<value_select_limit<<endl;
                for (auto b: leftTable->getSchema()->getAtts())
                    replace_all2(value_select_limit, tableAlias + "_" + b.first, b.first);

                if(a->hasAgg()){
                    aggregates.push_back(value_select_limit);
                }else{
                    attributes.push_back(value_select_limit);
                }

                //原来的顺序
                value_order.push_back(value_select_limit);
            }

            int i=0;
            //先att 再agg
            for(auto att : attributes){
                string att_name = "att_" + to_string (i++);
                att_index[att] = att_name;
                //替换alias后
                agg_Schema->getAtts().push_back(make_pair(att_name, myRec.getType(att)));
            }
            for (auto agg: aggregates)
            {
                string att_name = "att_" + to_string (i++);
                att_index[agg] = att_name;
                agg_Schema->getAtts ().push_back (make_pair (att_name, myRec.getType (agg)));
            }
//            cout << "agg schema: " << agg_Schema << "\n";

            for (auto value: value_order)
            {
                //按原来顺序的schema
                top_Schema->getAtts().push_back(make_pair(att_index[value], myRec.getType(value)));
                //替换alias后
                top_projection.push_back("[" + att_index[value] + "]");
//                cout << "Projection after aggregate might be: " << "[" + att_index[value] + "]" << endl;
            }


            LogicalOpPtr returnVal = make_shared <LogicalAggregate>
                    (leftTableScan,
                    make_shared <MyDB_Table> ("aggTable", "aggTableStorageLoc", agg_Schema),
                     make_shared<MyDB_Table>("outputTable", "outputTableLoc", top_Schema),
                            valuesToSelect,groupingClauses, true, tableAlias, top_projection);
            return returnVal;

        }else{

            for (auto b: leftTable->getSchema ()->getAtts ()) {
                bool needIt = false;
                for (auto a: valuesToSelect) {
                    if (a->referencesAtt (tableAlias, b.first)) {
                        needIt = true;
                        break;
                    }
                }
                if (needIt) {
                    leftSchema->getAtts ().push_back (make_pair (tableAlias + "_" + b.first, b.second));
                    leftExprs.push_back ("[" + b.first + "]");
//                    cout << "left expr: " << ("[" + b.first + "]") << "\n";
                }
            }

//            cout << "left schema: " << leftSchema << "\n";
            LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tableName],
                                                                         make_shared <MyDB_Table> ("scanTable", "scanStorageLoc", leftSchema),
                                                                         make_shared <MyDB_Stats> (leftTable, tableAlias), leftCNF, leftExprs, tableAlias);
            return leftTableScan;
        }


    }else if(tablesToProcess.size () == 2){//table >= 2
        // find the two input tables
        MyDB_TablePtr leftTable = allTables[tablesToProcess[0].first];
        MyDB_TablePtr rightTable = allTables[tablesToProcess[1].first];

        // find the various parts of the CNF
        vector <ExprTreePtr> leftCNF;
        vector <ExprTreePtr> rightCNF;
        vector <ExprTreePtr> topCNF;


	    // loop through all of the disjunctions and break them apart
        for (auto a: allDisjunctions) {
            bool inLeft = a->referencesTable (tablesToProcess[0].second);
            bool inRight = a->referencesTable (tablesToProcess[1].second);
            if (inLeft && inRight) {
//                cout << "top " << a->toString () << "\n";
                topCNF.push_back (a);
            } else if (inLeft) {
//                cout << "left: " << a->toString () << "\n";
                leftCNF.push_back (a);
            } else {
//                cout << "right: " << a->toString () << "\n";
                rightCNF.push_back (a);
            }
        }

        // now get the left and right schemas for the two selections
        MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
        MyDB_SchemaPtr rightSchema = make_shared <MyDB_Schema> ();
        MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
        vector <string> leftExprs;
        vector <string> rightExprs;
        vector <ExprTreePtr> totExprs;

	    // and see what we need from the left, and from the right
        //left schema只需要 出现于双table的，+ select中被选中的
	for (auto b: leftTable->getSchema ()->getAtts ()) {
		bool needIt = false;
		for (auto a: valuesToSelect) {
			if (a->referencesAtt (tablesToProcess[0].second, b.first)) {
				needIt = true;
                break;
			}
		}
		for (auto a: topCNF) {
			if (a->referencesAtt (tablesToProcess[0].second, b.first)) {
				needIt = true;
                break;
			}
		}
		if (needIt) {
			leftSchema->getAtts ().push_back (make_pair (tablesToProcess[0].second + "_" + b.first, b.second));
			totSchema->getAtts ().push_back (make_pair (tablesToProcess[0].second + "_" + b.first, b.second));
			leftExprs.push_back ("[" + b.first + "]");
//			cout << "left expr: " << ("[" + b.first + "]") << "\n";

            char *s1 = (char *)((tablesToProcess[0].second).c_str());
            char *s2 = (char *)((b.first).c_str());
//            cout << "*******" << endl;
//            cout << "join process, check totExprs attribute: left!" << endl;
//            cout << "s1" << s1 << endl;
//            cout << "s2" << s2 << endl;
//            cout << make_shared<Identifier>(s1, s2)->toString() << endl;

            totExprs.push_back (make_shared<Identifier>(s1, s2));
		}
	}

//	cout << "left schema: " << leftSchema << "\n";

	// and see what we need from the right, and from the right
	for (auto b: rightTable->getSchema ()->getAtts ()) {
		bool needIt = false;
		for (auto a: valuesToSelect) {
			if (a->referencesAtt (tablesToProcess[1].second, b.first)) {
				needIt = true;
			}
		}
		for (auto a: topCNF) {
			if (a->referencesAtt (tablesToProcess[1].second, b.first)) {
				needIt = true;
			}
		}
		if (needIt) {
			rightSchema->getAtts ().push_back (make_pair (tablesToProcess[1].second + "_" + b.first, b.second));
			totSchema->getAtts ().push_back (make_pair (tablesToProcess[1].second + "_" + b.first, b.second));
			rightExprs.push_back ("[" + b.first + "]");
//			cout << "right expr: " << ("[" + b.first + "]") << "\n";

            char *s1 = (char *)((tablesToProcess[1].second).c_str());
            char *s2 = (char *)((b.first).c_str());
//            cout << "*******" << endl;
//            cout << "join process, check totExprs attribute - right!" << endl;
//            cout << "s1" << s1 << endl;
//            cout << "s2" << s2 << endl;
//            cout << make_shared<Identifier>(s1, s2)->toString() << endl;

            totExprs.push_back (make_shared<Identifier>(s1, s2));
		}
	}
//	cout << "right schema: " << rightSchema << "\n";

	// now we gotta figure out the top schema... get a record for the top
	MyDB_Record myRec (totSchema);

	// and get all of the attributes for the output
	MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();
	int i = 0;
	for (auto a: valuesToSelect) {
		topSchema->getAtts ().push_back (make_pair ("att_" + to_string (i++), myRec.getType (a->toString ())));
	}
//	cout << "top schema: " << topSchema << "\n";

    // --- aggregate
    MyDB_SchemaPtr agg_Schema = make_shared <MyDB_Schema> ();
    MyDB_SchemaPtr top_Schema = make_shared <MyDB_Schema> ();
    vector<string> attributes;
    vector<string> aggregates;

    vector<string> value_order;
    vector<string> top_projection;
    unordered_map<string, string> att_index;

    for(auto a : valuesToSelect){
        string vts = a->toString ();
//        cout<< "valuesToSelect: "<< vts <<endl;
        //不需要替换了 双表
//        for (auto b: leftTable->getSchema()->getAtts())
//            replace_all2(vts, tableAlias + "_" + b.first,b.first)
        if(a->hasAgg()){
            aggregates.push_back(vts);
        }else{
            attributes.push_back(vts);
        }
        value_order.push_back(vts);
    }

    i = 0;
    for (auto att: attributes){
        string att_name = "att_" + to_string (i++);
        att_index[att] = att_name;
        agg_Schema->getAtts ().push_back (make_pair (att_name, myRec.getType (att)));
    }
    for (auto agg: aggregates){
        string att_name = "att_" + to_string (i++);
        att_index[agg] = att_name;
        agg_Schema->getAtts ().push_back (make_pair (att_name, myRec.getType (agg)));
    }
//    cout << "agg schema: " << agg_Schema << "\n";

    for (auto value: value_order){
        top_Schema->getAtts().push_back(make_pair(att_index[value], myRec.getType(value)));
        top_projection.push_back("[" + att_index[value] + "]");
//        cout << "Projection after aggregate might be: " << "[" + att_index[value] + "]" << endl;
    }

        // and it's time to build the query plan
	LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[0].first],
		make_shared <MyDB_Table> ("leftTable", "leftStorageLoc", leftSchema),
		make_shared <MyDB_Stats> (leftTable, tablesToProcess[0].second), leftCNF, leftExprs, tablesToProcess[0].second);

    LogicalOpPtr rightTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[1].first],
		make_shared <MyDB_Table> ("rightTable", "rightStorageLoc", rightSchema),
		make_shared <MyDB_Stats> (rightTable, tablesToProcess[1].second), rightCNF, rightExprs, tablesToProcess[1].second);

    if(!areAggs){

        LogicalOpPtr returnVal = make_shared <LogicalJoin> (leftTableScan, rightTableScan,
        make_shared <MyDB_Table> ("topTable", "topStorageLoc", topSchema),
        topCNF, valuesToSelect);
        return returnVal;

    }else{
        LogicalOpPtr joinOp = make_shared<LogicalJoin>(leftTableScan, rightTableScan,
            make_shared<MyDB_Table>("joinTable", "joinStorageLoc", totSchema),
            topCNF,totExprs);

        LogicalOpPtr agg_opp = make_shared<LogicalAggregate>(joinOp, make_shared<MyDB_Table>("aggTable", "aggTableLocation", agg_Schema), make_shared<MyDB_Table>("outputTable","outputTableLocation", top_Schema),valuesToSelect,
                                                             this->groupingClauses, false, tablesToProcess[0].second, top_projection);

        return agg_opp;
    }

    }else{// size > 2;
//        int size = tablesToProcess.size();
//        vector<pair<int, int>> tables;
//        int i = 0;
//        for(auto t : tablesToProcess){
//            tables.push_back(make_pair(i++, allTables[t.first]->getTupleCount()));
//        }
//        sort(tables.begin(),tables.end(),LessSort);//升序排列
//
//        cout<<"ascending sort"<<endl;
//        for(auto a:tables){
//            cout<<a.second<<endl;
//        }
//        MyDB_TablePtr leftTable = allTables[tablesToProcess[tables[0].first].first];
//        MyDB_TableReaderWriterPtr leftSpec = allTableReaderWriters[tablesToProcess[tables[0].first].first];
//
//        MyDB_TablePtr rightTable = allTables[tablesToProcess[tables[1].first].first];
//        MyDB_TableReaderWriterPtr rightSpec = allTableReaderWriters[tablesToProcess[tables[1].first].first];
//
//        string leftAlias = tablesToProcess[tables[0].first].second;
//        string rightAlias = tablesToProcess[tables[1].first].second;
//        vector <ExprTreePtr> leftCNF; // 只涉及left table的 where predicate
//        vector <ExprTreePtr> rightCNF;
//        vector <ExprTreePtr> topCNF;
//        for (auto a: allDisjunctions) {
//            bool inLeft = a->referencesTable (leftAlias);
//            bool inRight = a->referencesTable (rightAlias);
//            if (inLeft && inRight) {
//                cout << "top " << a->toString () << "\n";
//                topCNF.push_back (a);
//            } else if (inLeft) {
//                cout << "left: " << a->toString () << "\n";
//                leftCNF.push_back (a);
//            } else {
//                cout << "right: " << a->toString () << "\n";
//                rightCNF.push_back (a);
//            }
//        }
//        MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
//        MyDB_SchemaPtr rightSchema = make_shared <MyDB_Schema> ();
//        MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
//        vector <string> leftExprs;
//        vector <string> rightExprs;
//        vector <ExprTreePtr> totalExprs;
//        for(auto b : leftTable->getSchema()->getAtts()){
//            leftSchema->getAtts ().push_back (make_pair (leftAlias+ "_" + b.first, b.second));
//            totSchema->getAtts ().push_back (make_pair (leftAlias+ "_" + b.first, b.second));
//        }
//        for(auto b : rightTable->getSchema()->getAtts()){
//            rightSchema->getAtts ().push_back (make_pair (rightAlias+ "_" + b.first, b.second));
//            totSchema->getAtts ().push_back (make_pair (rightAlias+ "_" + b.first, b.second));
//        }
//        LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (leftSpec,
//                                                                     make_shared <MyDB_Table> ("leftTable1", "leftStorageLoc1", leftSchema),
//                                                                     make_shared <MyDB_Stats> (leftTable, leftAlias), leftCNF, leftExprs, leftAlias);
//
//        LogicalOpPtr rightTableScan = make_shared <LogicalTableScan> (rightSpec,
//                                                                      make_shared <MyDB_Table> ("rightTable1", "rightStorageLoc1", rightSchema),
//                                                                      make_shared <MyDB_Stats> (rightTable, rightAlias), rightCNF, rightExprs, rightAlias);
//        LogicalOpPtr returnVal = make_shared <LogicalJoin> (leftTableScan, rightTableScan,
//                                                            make_shared <MyDB_Table> ("topTable1", "topStorageLoc1", totSchema),
//                                                            topCNF, valuesToSelect);
//
//        MyDB_TableReaderWriterPtr tempRes = returnVal->execute();
//
//
//        return returnVal;
    }
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
