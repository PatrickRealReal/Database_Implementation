
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include <sstream>
#include "MyDB_LogicalOps.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "Aggregate.h"

void replace_all(string& s,string const& toReplace, string const& replaceWith) {
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

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection 
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
//LogicalOpPtr inputOp;
//MyDB_TablePtr outputSpec;
//MyDB_TablePtr final_output_Spec;
//vector <ExprTreePtr> exprsToCompute;
//vector <ExprTreePtr> groupings;
//bool is_single_table;
//string single_table_alias;
//vector<string> regular_select_projection;
MyDB_TableReaderWriterPtr LogicalAggregate :: execute () {
//    cout<< "\n";
//    cout<<"LogicalAggregate - execute:" << "\n";
//    cout<< "\n";
    MyDB_TableReaderWriterPtr inputTable = inputOp->execute();

    vector <pair <MyDB_AggType, string>> aggToCompute;
    vector<string> allGroupings;

    for(auto &valueToSelect : exprsToCompute){
        string valueStr = valueToSelect->toString();

        string agg_type = "";
        string expr_str = "";
        if(valueToSelect->hasAgg()){//agg列
//            cout<<"valueStr - hasAgg: " << valueStr <<"\n";
            if (valueStr.size() >= 5) {
                agg_type = valueStr.substr(0, 3);
                expr_str = valueStr.substr(4, valueStr.size() - 5);
//                cout<<"expr_str: "<<expr_str<<endl;
                for (auto b : inputTable->getTable()->getSchema()->getAtts()) {
                    replace_all(expr_str, single_table_alias+"_"+b.first, b.first);
                }
//                cout<<"expr_str - after: "<<expr_str<<endl;
            }
            if (agg_type == "sum") {
                aggToCompute.push_back(make_pair(MyDB_AggType :: sum, expr_str));
            } else if (agg_type == "avg") {
                aggToCompute.push_back(make_pair(MyDB_AggType :: avg, expr_str));
            }
        }else{
//            cout<<"valueStr - normal: " << valueStr <<"\n";
            //和grouping一样的？att一样 但是select可能 有自定义的内容，需要使用这个
            if (this->is_single_table){
                for (auto b: inputTable->getTable()->getSchema()->getAtts()){
                    replace_all(valueStr, this->single_table_alias + "_" + b.first, b.first);
                }
            }
//            cout << "The grouping item is: " << valueStr << endl;
            allGroupings.push_back(valueStr);
        }
    }


//    for (auto& grouping_clause : groupings) {
//        string goupingTemp = grouping_clause->toString();
//        for (auto b : inputTable->getTable()->getSchema()->getAtts()) {
//            replace_all(goupingTemp, single_table_alias+"_"+b.first, b.first);
//        }
////        cout<<"goupingTemp : " << goupingTemp <<"\n";
////        allGroupings.push_back(goupingTemp);
//    }



    //用的outputSpec 和 input的mgr
    MyDB_TableReaderWriterPtr aggTableOut = make_shared <MyDB_TableReaderWriter> (this->outputSpec, inputTable->getBufferMgr());
    Aggregate agg(inputTable, aggTableOut, aggToCompute, allGroupings, "bool[true]");
    agg.run();

//    cout << "!!!!!!!!!"<< "\n" << "Aggregate table: \n" <<endl;
//    MyDB_RecordPtr iterator_rec = aggTableOut->getEmptyRecord();
//    MyDB_RecordIteratorAltPtr iterator = aggTableOut->getIteratorAlt();
//    while (iterator->advance()){
//        iterator->getCurrent(iterator_rec);
//        cout << iterator_rec << "\n";
//    }
//    cout << "!!!!!!!!!"<< endl;


//    cout << "The schema for the output is:" << endl;
//    for (auto b: outputSpec->getSchema()->getAtts())
//        cout << b.first << " : " << b.second->toString()<< endl;
//
//    cout << "For regular aggregate order, the att selection is:" << endl;
//    for (auto a: this->regular_select_projection)
//        cout << a << endl;

    MyDB_TableReaderWriterPtr finalOutPut = make_shared<MyDB_TableReaderWriter>(this->final_output_Spec, inputTable->getBufferMgr());
    RegularSelection selection(aggTableOut, finalOutPut, "bool[true]", this->regular_select_projection);
    selection.run();

	return finalOutPut;
}

// we don't really count the cost of the aggregate, so cost its subplan and return that
pair <double, MyDB_StatsPtr> LogicalAggregate :: cost () {
	return inputOp->cost ();
}
	
// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair <double, MyDB_StatsPtr> LogicalJoin :: cost () {
	auto left = leftInputOp->cost ();
	auto right = rightInputOp->cost ();
	MyDB_StatsPtr outputStats = left.second->costJoin (outputSelectionPredicate, right.second);
	return make_pair (left.first + right.first + outputStats->getTupleCount (), outputStats);
}
	
// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
//
// leftInputOp: this is the input operation that we are reading from on the left
// rightInputOp: this is the input operation that we are reading from on the left
// outputSpec: this is the table (location and schema) that we are going to create by running the operation
//    note that when "cost" is called, statistics should be returned for each of the attributes in the schema
//    associated with outputSpec.  Note that each attribute in this output spec should have a 1-1 correspondence
//    with the expressions in exprsToCompute
// selectionPred: the selection predicate to execute using the join
// exprsToCompute: the various projections to compute... the first item in exprsToComput corresponds to the
//    first attribute in outputSpec, the second item in exprsToComput corresponds to the second attribute, etc.
//
//LogicalOpPtr leftInputOp;
//LogicalOpPtr rightInputOp;
//MyDB_TablePtr outputSpec;
//vector <ExprTreePtr> outputSelectionPredicate;
//vector <ExprTreePtr> exprsToCompute;
MyDB_TableReaderWriterPtr LogicalJoin :: execute () {
//    cout<< "\n";
//    cout<<"LogicalJoin - execute:" << "\n";
//    cout<< "\n";
    MyDB_TableReaderWriterPtr leftInput = leftInputOp->execute();
    MyDB_TableReaderWriterPtr rightInput = rightInputOp->execute();

    //把outputSelectionPredicate left and right  转成scanjoin需要的finalSelectionPredicate
    vector<string> allPredicates;
    for(auto &a : this->outputSelectionPredicate){
        allPredicates.push_back(a->toString());
    }
    string finalSelectionPredicate;
    if(allPredicates.empty()){
        finalSelectionPredicate = "bool[true]";
    }else if(allPredicates.size() == 1){
        finalSelectionPredicate = allPredicates[0];
    }else{
        finalSelectionPredicate = "&& (" + allPredicates[0] + ", " + allPredicates[1] + ")";
        for (int i = 2 ; i < allPredicates.size(); ++i) {
            finalSelectionPredicate = "&& (" + finalSelectionPredicate + ", " + allPredicates[i] + ")";
        }
    }

    //最后select的部分
    vector <string> projections;
    for (auto p: exprsToCompute){
        projections.push_back(p->toString());
    }

    //只取equal部分, 没有check左和右
    vector <pair<string,string>> equalityChecks;
    for (auto cnf: outputSelectionPredicate) {
        if (cnf->isEq()){
            pair<string, string> equalityString;
            ExprTreePtr lhs = cnf->getLHS();
            ExprTreePtr rhs = cnf->getRHS();
//            equalityString.first = lhs->toString();
//            equalityString.second = rhs->toString();
            bool isLeft = false;
            for (auto p : leftInput->getTable ()->getSchema ()->getAtts ()) {
                if (lhs->toString () == "[" + p.first + "]") {
                    isLeft = true;
                    break;
                }
            }
            if (isLeft) {
                equalityString.first = lhs->toString();
                equalityString.second = rhs->toString();
            }else {
                equalityString.second = lhs->toString();
                equalityString.first = rhs->toString();
            }
            equalityChecks.push_back(equalityString);
        }
    }

    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(this->outputSpec, leftInput->getBufferMgr());
    ScanJoin scanJoin(leftInput, rightInput, outputTable, finalSelectionPredicate, projections, equalityChecks, "bool[true]", "bool[true]");

    scanJoin.run();

	return outputTable;
}

// this costs the table scan returning the compute set of statistics for the output
pair <double, MyDB_StatsPtr> LogicalTableScan :: cost () {
	MyDB_StatsPtr returnVal = inputStats->costSelection (selectionPred);
	return make_pair (returnVal->getTupleCount (), returnVal);	
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
//MyDB_TableReaderWriterPtr inputSpec;
//MyDB_TablePtr outputSpec;
//MyDB_StatsPtr inputStats;
//vector <ExprTreePtr> selectionPred;
//vector <string> exprsToCompute;
//string alias;
MyDB_TableReaderWriterPtr LogicalTableScan :: execute () {
//    cout<< "\n";
//    cout<<"LogicalTableScan --- Execute, allPredicates:" << "\n";
//    cout<< "\n";
    vector<string> allPredicates;

    //need to delete the alias
    for(auto &a : this->selectionPred){
        string oriPredicate = a->toString();
        for (auto b : inputSpec->getTable()->getSchema()->getAtts()) {
            replace_all(oriPredicate, alias+"_"+b.first, b.first);
        }
        allPredicates.push_back(oriPredicate);
    }

    string selectionPredicate;

	if(allPredicates.empty()){
        selectionPredicate = "bool[true]";
    }else if(allPredicates.size() == 1){
        selectionPredicate = allPredicates[0];
    }else{
        selectionPredicate = "&& (" + allPredicates[0] + ", " + allPredicates[1] + ")";
        for (int i = 2 ; i < allPredicates.size(); ++i) {
            selectionPredicate = "&& (" + selectionPredicate + ", " + allPredicates[i] + ")";
        }
    }
//    cout<<"LogicalTableScan --- selectionPredicate" <<endl;
//    cout<<selectionPredicate<< "\n";
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(this->outputSpec, this->inputSpec->getBufferMgr());
    RegularSelection regularSelection(this->inputSpec, outputTable, selectionPredicate, this->exprsToCompute);

    regularSelection.run();

    return outputTable;
}



#endif
