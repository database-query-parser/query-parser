/**
 * page nested join algorithm
 **/

package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {


    private int batchsize;  //Number of tuples per out batch

    /**
     * The following fields are useful during execution of
     * * the NestedJoin operation
     **/
    private int leftindex;     // Index of the join attribute in left table
    private int rightindex;    // Index of the join attribute in right table

    private String rfname;    // The file name where the right table is materialize

    private static int filenum = 0;   // To get unique filenum for this operation

    private Batch outbatch;   // Output buffer
    private Batch leftbatch;  // Buffer for left input stream
    private Batch rightbatch;  // Buffer for right input stream

    private ObjectInputStream in; // File pointer to the right hand materialized file

    private int lcurs;    // Cursor for left side buffer
    private int rcurs;    // Cursor for right side buffer
    private int lbcurs;   // Cursor for group of left side buffers
    private int maxlbcurs; // Total size of group of left side buffers

    private boolean eosl;  // Whether end of stream (left table) is reached
    private boolean eosr;  // End of stream (right table)
    private boolean eoslb; // End of stream (end of group of left buffers) is reached

    private int leftBufferSize;

    private ExternalSort leftSort;
    private ExternalSort rightSort;

    private List<File> sortedLeftFiles;
    private List<File> sortedRightFiles;

    private final String SORTED_LEFT_FILE_NAME = "SMJ-Left";
    private final String SORTED_RIGHT_FILE_NAME = "SMJ-Right";

    private boolean hasMatch;
    private int rightFirstMatchIndex;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /**
     * During open finds the index of the join attributes
     * *  Materializes the right hand side into a file
     * *  Opens the connections
     **/


    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        // Sorts both left and right relations
        leftSort = new ExternalSort(left, leftindex, numBuff);
        rightSort = new ExternalSort(right, rightindex, numBuff);

        if (!(leftSort.open() && rightSort.open())) {
            return false;
        }

        try {
            sortedLeftFiles = writeSortedFiles(leftSort, SORTED_LEFT_FILE_NAME);
            sortedRightFiles = writeSortedFiles(rightSort, SORTED_RIGHT_FILE_NAME);
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error in writing sorted files");
            return false;
        }

        leftSort.close();
        rightSort.close();

        hasMatch = false;
        rightFirstMatchIndex = 0;

        leftBufferSize = numBuff - 2; // 1 buffer for right, 1 for output, remaining for left
        // right will probe left

        /** initialize the cursors of input buffers **/

        lcurs = 0;
        rcurs = 0;
        lbcurs = 0;
        maxlbcurs = (int) Math.ceil(sortedLeftFiles.size() / leftBufferSize);
        eosl = false;
        eosr = true;
        eoslb = false;

        return true;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("NestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {

            if (lcurs == 0 && eosr == true) {
                /** new left page is to be fetched**/
                leftbatch = (Batch) left.next();
                if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
                /** Whenver a new left page came , we have to start the
                 ** scanning of right table
                 **/
                try {

                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }

            }

            while (eosr == false) {

                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    i = lcurs;
                    j = rcurs;

                    while (i < leftbatch.size() && j < rightbatch.size()) {
                        Tuple lefttuple = leftbatch.elementAt(i);
                        Tuple righttuple = rightbatch.elementAt(j);
                        int condition = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                        if (condition == 0) {
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            //Debug.PPrint(outtuple);
                            //System.out.println();
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 1
                                    lcurs = 0;
                                    rcurs = 0;
                                } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 2
                                    lcurs = i + 1;
                                    rcurs = 0;
                                } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {//case 3
                                    lcurs = i;
                                    rcurs = j + 1;
                                } else {
                                    lcurs = i;
                                    rcurs = j + 1;
                                }
                                return outbatch;
                            }

                            if (!hasMatch) {
                                rightFirstMatchIndex = j;
                                hasMatch = true;
                            }
                            j++;
                        } else if (condition < 0) { // left is smaller
                            i++;
                            if (hasMatch) {
                                j = rightFirstMatchIndex;
                            }
                            hasMatch = false;
                        } else { // left is bigger
                            j++;
                            hasMatch = false;
                        }

                        if (j < rightbatch.size())
                            rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {

        clearTempFiles(sortedLeftFiles);
        clearTempFiles(sortedRightFiles);

        return true;

    }

    /*
    private Vector<Batch> readLeftInput(int start, int end, List<File> fileList) {
        Vector<Batch> batchList = new Vector<>();
        if (end > fileList.size()) {
            end = fileList.size();
        }

        for (int i = start; i < end; i++) {
            File file = fileList.get(i);
            Batch batch;
            try {
                leftInput = new ObjectInputStream(new FileInputStream(file));
                batch = (Batch) leftInput.readObject();
                leftInput.close();
                batchList.add(batch);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: IOException in reading sortedLeftFiles");
            } catch (ClassNotFoundException cnfe) {
                System.out.println("SortMergeJoin: ClassNotFoundException in deserialization");
            }
        }

        return batchList;
    }

    private Batch readRightInput(int index, List<File> fileList) {
        Batch batch = null;
        File file = fileList.get(index);
        try {
            rightInput = new ObjectInputStream(new FileInputStream(file));
            batch = (Batch) rightInput.readObject();
            rightInput.close();
        } catch (IOException ioe) {
            System.out.println("SortMergeJoin: IOException in reading sortedRightFiles at next()");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("SortMergeJoin: ClassNotFoundException in deserialization");
        }
        return batch;
    }
*/

    private List<File> writeSortedFiles(Operator op, String filePrefix) throws IOException {
        Batch batch;
        int num = 0;
        List<File> files = new Vector<>();
        while ((batch = op.next()) != null) {
            File file = new File(filePrefix + num);
            num++;
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(batch);
            files.add(file);
            out.close();
        }
        return files;
    }


    /**
     * Clearing up temporary files
     */
    private void clearTempFiles(List<File> files) {
        for (File file : files) {
            file.delete();
        }
    }


}











































