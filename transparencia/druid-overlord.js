/**
 * Created by clayton on 23/06/16.
 */
( () => {
    'use strict';

    const receita = require( './jobs/receita.json' );
    const despesa = require( './jobs/despesa.json' );
    const rp = require( 'request-promise' );
    const job = process.arg[ 1 ] ? process.arg[ 1 ] : 'receita';
    const HOST = process.arg[ 2 ] ? process.arg[ 2 ] : 'http://overlord.labs.prodest.dcpr.es.gov.br';
    const year = process.arg[ 3 ] ? process.arg[ 3 ] : 2009;

  /**
   * tratando o body para aplicar a data
   *
   **/
    let bodyJson = job === 'receita' ? receita : despesa;
    let dateIni = new Date( Date.UTC( parseInt( year ), 0 ) );
    let dateEnd = new Date();
    bodyJson.dataSchema.granularitySpec.intervals = [ `${dateIni.toISOString().substring( 0, 10 )}/${dateEnd.toISOString().substring( 0, 10 )}` ];

    let options = {
        method: 'POST',
        uri: HOST,
        body: bodyJson,
        json: true // Automatically stringifies the body to JSON
    };

    rp( options )
    .then( function( data ) {
        console.info( data );
        process.exit( 1 );
    } )
    .catch( ( err ) => {
        console.error( err.message || err );
        process.exit( -1 );
    } );
} )();
