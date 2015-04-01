var _ = require( "lodash" );

function normalize( records ) {
	return _.map( records, function( _r ) {

		var rs = _.isArray( _r ) ? _r : [ _r ];

		return _.map( rs, function( a ) {
			//return _.omit( a, "vclock" );
			delete a.vclock;
			return a;
		} );

	} );
}

function normalizeEvents( records ) {
	var normalized = _.map( records, function( actor ) {

		actor.events = normalize( actor.events );

		actor.events = _.map( actor.events, function( e ) {

			e = _.map( e, function( _e ) {
				if ( _e._indexes.actor_id instanceof Buffer ) {
					_e._indexes.actor_id = _e._indexes.actor_id.toJSON();
				}
				return _e;
			} );

			e = _.sortBy( e, function( _e ) {
				return _e.id;
			} );

			return e;
			//return JSON.stringify( e );
		} );

		return actor;
	} );

	normalized = _.sortBy( normalized, function( actor ) {
		return actor.actor_id;
	} );

	return normalized;
}

module.exports = {
	normalize: normalize,
	normalizeEvents: normalizeEvents
};