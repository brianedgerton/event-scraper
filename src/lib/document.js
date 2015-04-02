var _ = require( "lodash" );

var deserializers = {
	"buffer": function( obj ) {
		return new Buffer( obj.data );
	}
};

function isSerialized( obj ) {
	return _.isEqual( _.keys( obj ), [ "type", "data" ] );
}

function deserialize( _doc ) {

	var doc = _.reduce( _doc, function( memo, value, key ) {
		if ( _.isPlainObject( value ) ) {
			if ( isSerialized( value ) ) {
				var type = value.type.toLowerCase();
				if ( deserializers[ type ] ) {
					memo[ key ] = deserializers[ type ]( value );
				} else {
					memo[ key ] = deserialize( value );
				}
			} else {
				memo[ key ] = deserialize( value );
			}
		} else {
			memo[ key ] = value;
		}

		return memo;
	}, {} );

	return doc;

}

function normalizeIndexes( indexes ) {
	return _.reduce( indexes, function( memo, value, key ) {
		memo[ key ] = value.toString();
		return memo;
	}, {} );
}

function prepare( rawDoc ) {

	var doc = deserialize( rawDoc );
	var indexes;

	if ( doc._indexes ) {
		indexes = normalizeIndexes( doc._indexes );
		delete doc._indexes;
	}

	return { data: doc, indexes: indexes };
}

function areSiblings( docs ) {
	if ( !_.isArray( docs ) ) {
		return false;
	}

	var vclocks = _.compact( _.pluck( docs, "vclock" ) );

	return vclocks.length === docs.length;
}

module.exports = {
	deserialize: deserialize,
	prepare: prepare,
	areSiblings: areSiblings
};