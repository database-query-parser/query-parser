package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;

public class GreedyOptimizer {

    private SQLQuery sqlquery;
    private int numJoin;

    private Vector projectList;
    private Vector fromList;
    private Vector selectionList;
    private Vector joinList;
    private Vector groupByList;

    private Hashtable tab_op_hash;
    private Operator root;

    public GreedyOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
        this.projectList = sqlquery.getProjectList();
        this.fromList = sqlquery.getFromList();
        this.selectionList = sqlquery.getSelectionList();
        this.joinList = sqlquery.getJoinList();
        this.groupByList = sqlquery.getGroupByList();
        this.numJoin = joinList.size();
    }

    /** Create Scan Operator for each of the table
     ** mentioned in from list
     **/
    private void createScanOp() {
        int numtab = fromList.size();
        Scan tempOp = null;

        for (int i = 0; i < numtab; i++) {
            String tabname = (String) fromList.elementAt(i);
            Scan op = new Scan(tabname, OpType.SCAN);
            tempOp = op;

            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/
            String fileName = tabname + ".md";
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
                Schema schema = (Schema) ois.readObject();
                op.setSchema(schema);
                ois.close();
            } catch (Exception e) {
                System.err.println("GreedyOptimizer:Error reading Schema of the table" + fileName);
                System.exit(1);
            }
            tab_op_hash.put(tabname, op);
        }

        if (selectionList.size() == 0) {
            root = tempOp;
        }
    }

    /** Create Selection Operators for each of the
     ** selection condition mentioned in Condition list
     **/
    private void createSelectOp() {
        Select op = null;

        for (int i = 0; i < selectionList.size(); i++) {
            Condition con = (Condition) selectionList.elementAt(i);
            if (con.getOpType() == Condition.SELECT) {
                String tabname = con.getLhs().getTabName();
                Operator tempOp = (Operator) tab_op_hash.get(tabname);
                op = new Select(tempOp, con, OpType.SELECT);
                /** set the schema same as base relation **/
                op.setSchema(tempOp.getSchema());

                modifyHashtable(tempOp, op);
            }
        }

        if (selectionList.size() != 0) {
            root = op;
        }
    }

    private void createJoinOp() {

        Operator rootWithMinCost = null;
        Operator temp = null;
        int minCost = Integer.MAX_VALUE;
        PlanCost pc;
        Join jn = null;
        for (int i = 0; i < joinList.size(); i++) {
            Condition con = (Condition) joinList.get(i);
            String lefttab = con.getLhs().getTabName();
            String righttab = ((Attribute) con.getRhs()).getTabName();

            Operator left = (Operator) tab_op_hash.get(lefttab);
            Operator right = (Operator) tab_op_hash.get(righttab);

            jn = new Join(left, right, con, OpType.JOIN);
            Schema newSchema = left.getSchema().joinWith(right.getSchema());
            jn.setSchema(newSchema);

            int numJoinMethod = JoinType.numJoinTypes();
            System.out.println("Number of join methods: " + numJoinMethod);
            for (int j = 0; j < numJoinMethod; j++) {
                jn.setJoinType(j);
                modifyHashtable(left,jn);
                modifyHashtable(right,jn);
                temp = jn;

                pc = new PlanCost();
                int cost = pc.getCost(temp);
                System.out.println(cost);
                if (cost < minCost) {
                    minCost = cost;
                    rootWithMinCost = temp;
                }
            }
        }
        System.exit(2);
    }

    private void createProjectOp() {

    }

    public Operator preparePlan() {
        tab_op_hash = new Hashtable();

        createScanOp();
        createSelectOp();
        if (numJoin != 0) {
            createJoinOp();
        }
        createProjectOp();

        return root;
    }

    public Operator getOptimizedPlan() {
        Operator plan = preparePlan();
        int MINCOST = Integer.MAX_VALUE;
        Operator finalPlan = null;
        return finalPlan;
    }

    public int getNumJoin() {
        return numJoin;
    }

    private void modifyHashtable(Operator old, Operator newOp) {
        Enumeration e = tab_op_hash.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            Operator temp = (Operator) tab_op_hash.get(key);
            if (temp == old) {
                tab_op_hash.put(key, newOp);
            }
        }
    }
}
