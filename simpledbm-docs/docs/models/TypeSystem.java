
/**
 * @opt operations
 * @hidden
 */
class UMLOptions {}

interface Comparable {}
interface Storable {}

interface IndexKey extends Comparable, Storable {}

/**
 * @has 1 - 1..* DataValue
 */
interface Row extends IndexKey {}

interface TypeDescriptor extends Storable {}

/**
 * @assoc * - 1 TypeDescriptor
 */
interface DataValue extends Storable, Comparable {
}

/**
 * @navassoc - <create> - TypeDescriptor
 * @navassoc - <create> - DataValue
 */
interface TypeFactory {}

interface IndexKeyFactory {}

/**
 * @navassoc - <create> - Row
 * @navassoc - <uses> - TypeFactory
 */
interface RowFactory extends IndexKeyFactory {
	void registerRowType(int Id, TypeDescriptor[] types);
}

