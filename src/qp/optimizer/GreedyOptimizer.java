package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import qp.operators.Debug;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
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

    private Vector tempJoinList;

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

        int minCost = Integer.MAX_VALUE;
        PlanCost pc;
        Join jn = null;
        tempJoinList = (Vector) joinList.clone();

        int currJoinOp = 0;
        int tempJoinIndex = 0;
        int tempJoinMethodIndex = 0;
        Operator left = null;
        Operator right = null;

        int[] joinSelected = new int[joinList.size()];

        while (currJoinOp < numJoin) {
            System.out.println("===== Iteration " + (currJoinOp+1) + " =====");
            minCost = Integer.MAX_VALUE;
            for (int i = 0; i < joinList.size(); i++) {
                if (joinSelected[i] == 1)
                    continue;
                Condition con = (Condition) joinList.get(i);
                String lefttab = con.getLhs().getTabName();
                String righttab = ((Attribute) con.getRhs()).getTabName();

                //System.out.println(lefttab + " " + righttab);

                left = (Operator) tab_op_hash.get(lefttab);
                right = (Operator) tab_op_hash.get(righttab);

                jn = new Join(left, right, con, OpType.JOIN);
                Schema newSchema = left.getSchema().joinWith(right.getSchema());
                jn.setSchema(newSchema);

                int numJoinMethod = JoinType.numJoinTypes();
                for (int j = 0; j < numJoinMethod; j++) {
                    jn.setJoinType(j);

                    Debug.PPrint(jn);
                    pc = new PlanCost();
                    int cost = pc.getCost(jn);
                    System.out.println(" " + cost);
                    if (cost < minCost) {
                        tempJoinIndex = i;
                        tempJoinMethodIndex = j;
                        minCost = cost;
                    }
                }
            }
            System.out.println("----- End of Iteration " + (currJoinOp+1) + " -----");
            System.out.println("Iteration " + (currJoinOp+1) + " Minimum Cost: " + minCost);

            modifyJoinOp(tempJoinIndex, tempJoinMethodIndex);
            joinSelected[tempJoinIndex] = 1;
            currJoinOp++;
        }

        if (numJoin != 0) {
            root = jn;
            System.out.println("===== End of Iteration =====");
            System.out.println("Min cost: " + minCost);
        }
    }

    private void createProjectOp() {
        Operator base = root;
        if ( projectList == null )
            projectList = new Vector();

        if(!projectList.isEmpty()){
            root = new Project(base,projectList,OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectList);
            root.setSchema(newSchema);
        }
        System.exit(2);
    }

    private void modifyJoinOp(int listIndex, int methodIndex) {
        Condition con = (Condition) joinList.get(listIndex);
        String lefttab = con.getLhs().getTabName();
        String righttab = ((Attribute) con.getRhs()).getTabName();

        Operator left = (Operator) tab_op_hash.get(lefttab);
        Operator right = (Operator) tab_op_hash.get(righttab);

        Join jn = new Join(left, right, con, OpType.JOIN);
        Schema newSchema = left.getSchema().joinWith(right.getSchema());
        jn.setSchema(newSchema);
        jn.setJoinType(methodIndex);

        System.out.print("Selected: ");
        Debug.PPrint(jn);
        System.out.println();

        modifyHashtable(left,jn);
        modifyHashtable(right,jn);
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
