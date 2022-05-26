public class SQLTerm {

    String _strTableName=null;
    String _strColumnName=null;
    String _strOperator=null;
    Object _objValue=null;

    public SQLTerm(){

    }

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }
}