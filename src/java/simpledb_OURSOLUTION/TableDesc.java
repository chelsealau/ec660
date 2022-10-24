package simpledb_OURSOLUTION;

import simpledb.DbFile;

/**
 * TableDesc
 * 
 * Simple organization class to hold information about a table.
 */
public class TableDesc {
  public DbFile dbFile;
  public String tableName;
  public String tablePKey;
  

  public TableDesc(String tableName) {
    this.dbFile = null;
    this.tableName = tableName;
    this.tablePKey = null;
  }
  public TableDesc(DbFile dbFile, String tableName, String tablePKey) {
    this.dbFile = dbFile;
    this.tableName = tableName;
    this.tablePKey = tablePKey;
  }



  
  // /** 
  //  * Overriding equality to prevent deep search of Files when we only care about table name
  //  * @param obj: TableDesc to compare this with when searching for the DbFileID
  //  * @return boolean
  //  */
  // @Override
  // public boolean equals(Object obj) {
  //   if (obj == this) {
  //     return true;
  //   }

  //   if (!(obj instanceof TableDesc)) {
  //     return false;
  //   }

  //   TableDesc td = (TableDesc) obj;
  //   return this.tableName == td.tableName;
  // }
}