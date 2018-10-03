package com.clairvoyantsoft.flink.Utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.log4j.Logger;

import com.clairvoyantsoft.flink.Sink.KuduOutputFormat;
import com.clairvoyantsoft.flink.Utils.Exceptions.KuduClientException;
import com.clairvoyantsoft.flink.Utils.Exceptions.KuduTableException;

public class Utils {

    //Kudu variables
    private KuduClient client;
    private KuduSession session;

    // LOG4J
    private final static Logger logger = Logger.getLogger(Utils.class);

    /**
     * Builder Util Class which creates a Kudu client and log in to be able to perform operations later
     * @param host Kudu's host
     * @throws KuduClientException In case of exception caused by Kudu Client
     */
    public Utils(String host) throws KuduClientException {
        this.client = new KuduClient.KuduClientBuilder(host).build();
        if (client == null){
            throw new KuduClientException("ERROR: param \"host\" not valid, can't establish connection");
        }
        this.session = this.client.newSession();
    }

    /**
     * Return an instance of the table indicated in the settings
     *
     * In case that the table exists, return an instance of the table
     * In case that the table doesn't exist, create a new table with the data provided and return an instance
     * In both cases,takes into account the way of the table to perfom some operations or others
     *
     *     If the mode is CREATE:
     *
     *         If the table exists: return error (Can not create table that already exists)
     *         If the table doesn't exist and  the list of column names has not been provided: return error
     *         If the table doesn't exist and  the list of column names has been provided: create a new table with data provided and return an instance
     *
     *    If the mode is APPEND:
     *
     *        If the table exists: return the instance in the table
     *        If the table doesn't exist: return error
     *
     *    If the mode is OVERRIDE:
     *
     *        If the table exist: delete all rows of this table and return an instance of it
     *        If the table doesn't exist: return error
     *
     *
     * @param tableName             Table name to use
     * @param tableMode             Operations mode for operate with the table (CREATE, APPEND, OVERRIDE)
     * @return                      Instance of the table indicated
     * @throws KuduTableException   In case of can't access to a table o can't create it (wrong params or not existing table)
     * @throws KuduException        In case of error of Kudu
     */
    public KuduTable useTable(String tableName, Integer tableMode) throws KuduTableException, KuduException {
        KuduTable table;

        if (tableMode == KuduOutputFormat.CREATE) {
            logger.error("Bad call method, use useTable(String tableName, String [] fieldsNames, RowSerializable row) instead");
            table = null;
        }else if (tableMode == KuduOutputFormat.APPEND) {
            logger.info("Modo APPEND");
            try {
                if (client.tableExists(tableName)) {
                    //logger.info("SUCCESS: There is the table with the name \"" + tableName + "\"");
                    table = client.openTable(tableName);
                } else {
                    logger.error("ERROR: The table doesn't exist");
                    throw new KuduTableException("ERROR: The table doesn't exist, so can't do APPEND operation");
                }
            } catch (Exception e) {
                throw new KuduTableException("ERROR: param \"host\" not valid, can't establish connection");
            }
        }else if (tableMode == KuduOutputFormat.OVERRIDE) {
            logger.info("Modo OVERRIDE");
            try {
                if (client.tableExists(tableName)) {
                    logger.info("SUCCESS: There is the table with the name \"" + tableName + "\". Emptying the table");
                    clearTable(tableName);
                    table = client.openTable(tableName);
                } else {
                    logger.error("ERROR: The table doesn't exist");
                    throw new KuduTableException("ERROR: The table doesn't exist, so can't do OVERRIDE operation");
                }
            } catch (Exception e) {
                throw new KuduTableException("ERROR: param \"host\" not valid, can't establish connection");
            }
        }else {
            throw new KuduTableException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableMode\" parameter.");
        }
        return table;
    }

    /**
     * Returns an instance of the table requested in parameters
     * If the table exists, returns an instance of the table
     * If the table doesn't exist, creates a new table with the data provided and returns an instance
     *
     * @param tableName     Table name to use
     * @param fieldsNames   List of names of columns of the table (to create table)
     * @param row           List of values to insert a row in the table (to know the types of columns)
     * @return              Instance of the table indicated
     * @throws IllegalArgumentException In case of wrong parameters
     * @throws KuduException    In case of exception caused by Kudu
     */
    public KuduTable useTable(String tableName, String [] fieldsNames, RowSerializable row) throws IllegalArgumentException, KuduException {
        KuduTable table;

        if (client.tableExists(tableName)){
            logger.info("The table exists");
            table = client.openTable(tableName);
        } else {
            if (tableName == null || tableName.equals("")) {
                throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableName\" parameter.");

            } else if (fieldsNames == null || fieldsNames[0].isEmpty()) {
                throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Missing \"fields\" parameter.");

            } else if (row == null){
                throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"row\" parameter.");

            } else {
                logger.info("The table doesn't exist");
                table = createTable(tableName, fieldsNames, row);
            }
        }
        return table;
    }
    /**
     * Create a new Kudu table and return the instance of this table
     *
     * @param tableName     name of the table to create
     * @param fieldsNames   list name columns of the table
     * @param row           list of values to insert a row in the table( to know the types of columns)
     * @return              instance of the table indicated
     * @throws KuduException In case of exception caused by Kudu
     */
    public KuduTable createTable (String tableName, String [] fieldsNames, RowSerializable row) throws KuduException {

        if(client.tableExists(tableName))
            return client.openTable(tableName);


        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>(); // Primary key
        rangeKeys.add(fieldsNames[0]);

        logger.info("Creating the table \"" + tableName + "\"...");
        for (int i = 0; i < fieldsNames.length; i++){
            ColumnSchema col;
            String colName = fieldsNames[i];
            Type colType = getRowsPositionType(i, row);

            if (colName.equals(fieldsNames[0])) {
                col = new ColumnSchemaBuilder(colName, colType).key(true).build();
                columns.add(0, col);//To create the table, the key must be the first in the column list otherwise it will give a failure
            } else {
                col = new ColumnSchemaBuilder(colName, colType).build();
                columns.add(col);
            }
        }
        Schema schema = new Schema(columns);

        if(!client.tableExists(tableName))
            client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4));
        //logger.info("SUCCESS: The table has been created successfully");


        return client.openTable(tableName);
    }
    /**
     * Delete the indicated table
     *
     * @param tableName name table to delete
     */
    public void deleteTable (String tableName){

        logger.info("Deleting the table \"" + tableName + "\"...");
        try {
            if(client.tableExists(tableName)) {
                client.deleteTable(tableName);
                logger.info("SUCCESS: Table deleted successfully");
            }
        } catch (KuduException e) {
            logger.error("The table \"" + tableName  +"\" doesn't exist, so can't be deleted.", e);
        }
    }

    /**
     * Return the type of the value of the position "pos", like the class object "Type"
     *
     * @param pos   Row position
     * @param row   list of values to insert a row in the table
     * @return      element type "pos"-esimo of "row"
     */
    public Type getRowsPositionType (int pos, RowSerializable row){
        return TableUtils.mapToType(row.productElement(pos).getClass());
    }

    /**
     * Return a list with all rows of the indicated table
     *
     * @param tableName Table name to read
     * @return          List of rows in the table(object Row)
     * @throws KuduException In case of exception caused by Kudu
     */
    public List<RowSerializable> readTable (String tableName) throws KuduException {

        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table).build();
        //Obtain the column name list
        String[] columnsNames = getNamesOfColumns(table);
        //The list return all rows
        List<RowSerializable> rowsList = new ArrayList<>();

        int posRow = 0;
        while (scanner.hasMoreRows()) {
            for (RowResult row : scanner.nextRows()) { //Get the rows
                RowSerializable rowToInsert = new RowSerializable(columnsNames.length);
                for (String col : columnsNames) { //For each column, it's type determined and this is how to read it
                    rowToInsert.setField(posRow, TableUtils.valueFromRow(row, col));
                    posRow++;
                }
                rowsList.add(rowToInsert);
                posRow = 0;
            }
        }
        return rowsList;
    }



    /**
     * Return a list with all rows of the indicated table
     *
     * @param tableName Table name to read
     * @throws KuduException In case of exception caused by Kudu
     */
    public void readTablePrint (String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table).build();
        int cont = 0;
        try {
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(result.rowToString());
                    cont++;
                }
            }
            System.out.println("Number of rows: " + cont);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns a representation on the table screen of a table
     *
     * @param row   row to show
     * @return      a string containing the data of the row indicated in the parameter
     */
    public String printRow (RowSerializable row){
        String res = "";
        for(int i = 0; i< row.productArity(); i++){
            res += (row.productElement(i) + " | ");
        }
        return res;
    }

    /**
     * Deelte all rows of the table until empty
     *
     * @param tableName  table name to empty
     * @throws KuduException In case of exception caused by Kudu
     */
    public void clearTable (String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        List<RowSerializable> rowsList = readTable(tableName);

        String primaryKey = table.getSchema().getPrimaryKeyColumns().get(0).getName();
        List<Delete> deletes = new ArrayList<>();
        for(RowSerializable row : rowsList){
            Delete d = table.newDelete();
            Type type = getRowsPositionType(0, row);
            PartialRow partialRow = d.getRow();
            TableUtils.valueToRow(partialRow, type, primaryKey, row.productElement(0));
            deletes.add(d);
        }
        for(Delete d : deletes){
            session.apply(d);
        }
        logger.info("SUCCESS: The table has been emptied successfully");
    }

    /**
     * Return a list of columns names in a table
     *
     * @param table table instance
     * @return      List of column names in the table indicated in the parameter
     */
    public String [] getNamesOfColumns (KuduTable table){
        List<ColumnSchema> columns = table.getSchema().getColumns();
        List<String> columnsNames = new ArrayList<>(); //  List of column names
        for (ColumnSchema schema : columns) {
            columnsNames.add(schema.getName());
        }
        String [] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }

    public boolean checkNamesOfColumns(String [] tableNames, String [] providedNames) throws KuduTableException{
        boolean res = false;
        if(tableNames.length != providedNames.length){
            res = false;
        } else{
            for (int i = 0; i < tableNames.length; i++) {
                res = tableNames[i].equals(providedNames[i]) ? true : false;
            }
        }
        if(!res){
            throw new KuduTableException("ERROR: The table column names and the provided column names don't match");
        }
        return res;
    }

    public void insert (KuduTable table, RowSerializable row, String [] fieldsNames) throws KuduException, NullPointerException {

        Insert insert;
        try{
            insert = table.newInsert();
        } catch (NullPointerException e){
            throw new NullPointerException("Error encountered at opening/creating table");
        }

        for (int index = 0; index < row.productArity(); index++){
            //Create the insert with the previous data in function of the type ,a different "add"
            Type type = getRowsPositionType(index, row);
            PartialRow partialRow = insert.getRow();
            TableUtils.valueToRow(partialRow, type, fieldsNames[index], row.productElement(index));
        }
        //When the Insert is complete, write it in the table
        session.apply(insert);
    }

    /**
     * Returns an instance of the kudu client
     *
     * @return  Kudu client
     */
    public KuduClient getClient() {
        return client;
    }

}