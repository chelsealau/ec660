package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    // Integer key will come from TableDesc.dbFile
    private ConcurrentHashMap<Integer, TableDesc> directory;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here (__done__)
        directory = new ConcurrentHashMap<Integer, TableDesc>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * 
     * @param file      the contents of the table to add; file.getId() is the
     *                  identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and
     *                  getFile
     * @param name      the name of the table -- may be an empty string. May not be
     *                  null. If a name
     *                  conflict exists, use the last table to be added as the table
     *                  for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        /*
         * Steps to carry out:
         * 1. Compute and save the DbFile ID
         * (check if the file already exists)
         * 2. Check if the table name already exists in the directory
         * (perform name conflict resolution)
         * 3. Create a TableDesc for the passed parameters
         * (use saved DbFile ID for directory key)
         * 4. Add the <DbFileID, TableDesc> pair to the directory
         */

        int newTableID = file.getId();
        // duplicate fileIDs found, we reset the existing entry to the newly passed
        if (directory.containsKey(newTableID)) {
            TableDesc existingDetails = directory.get(newTableID);
            existingDetails.dbFile = file;
            existingDetails.tableName = name;
            existingDetails.tablePKey = pkeyField;
        } else {
            directory.forEach((tableId, tableDetails) -> {
                // check if naming conflict exists
                if (tableDetails.tableName.equals(name)) {
                    tableDetails.dbFile = file;
                    tableDetails.tablePKey = pkeyField;
                    return;
                }
            });
            // no identical ids or naming conflicts found, so simply add
            directory.put(file.getId(), new TableDesc(file, name, pkeyField));
        }

    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * 
     * @param file the contents of the table to add; file.getId() is the identfier
     *             of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        for (Map.Entry<Integer, TableDesc> entry : directory.entrySet()) {
            if (entry.getValue().tableName.equals(name)) {
                return entry.getKey();
            }
        }
        throw new NoSuchElementException("Table does not exist");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if (!this.directory.containsKey(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return this.directory.get(tableid).dbFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        if (!this.directory.containsKey(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return this.directory.get(tableid).dbFile;
    }

    /**
     * Returns the primary key field of the specified table.
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @return the name of the primary key field
     * @throws NoSuchElementException if the table doesn't exist
     */
    public String getPrimaryKey(int tableid) throws NoSuchElementException {
        if (!this.directory.containsKey(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return this.directory.get(tableid).tablePKey;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here (__done__)
        return this.directory.keySet().iterator();
    }

    /**
     * Returns the name of the table with the specified id.
     * 
     * @param id The id of the table, as specified by the DbFile.getId()
     *           function passed to addTable
     * @return the name of the table
     * @throws NoSuchElementException if the table doesn't exist
     */
    public String getTableName(int id) throws NoSuchElementException {
        // some code goes here (__done__)
        if (!this.directory.containsKey(id)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return this.directory.get(id).tableName;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here (90% __done__)
        this.directory.forEach((tableId, tableDetails) -> {
            // Need to add this method to DbFile so it can clear the contents of the file
            // tableDetails.dbFile.clear();
        });
        this.directory.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
