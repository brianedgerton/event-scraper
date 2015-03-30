var chai = require( "chai" );
global.sinon = require( "sinon" );

chai.use( require( "chai-as-promised" ) );
chai.use( require( "sinon-chai" ) );
require( "sinon-as-promised" );
global.should = chai.should();