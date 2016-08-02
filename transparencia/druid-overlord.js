/**
 * Created by clayton on 23/06/16.
 */
( () => {
    'use strict';

    const receita = require( './jobs/receita.json' );
    const despesa = require( './jobs/despesa.json' );
    const rp = require( 'request-promise' );
    const job = process.argv[ 2 ] || 'receita';
    const HOST = process.argv[ 3 ] || 'http://overlord.labs.prodest.dcpr.es.gov.br/druid/indexer/v1/task';
    const year = process.argv[ 4 ] || 2009;

  /**
   * tratando o body para aplicar a data
   *
   **/
    let bodyJson = job === 'receita' ? receita : despesa;
    let dateIni = new Date( Date.UTC( parseInt( year ), 0 ) );
    let dateEnd = new Date();
    bodyJson.spec.dataSchema.granularitySpec.intervals = [ `${dateIni.toISOString().substring( 0, 10 )}/${dateEnd.toISOString().substring( 0, 10 )}` ];

    let options = {
        method: 'POST',
        uri: HOST,
        body: bodyJson,
        json: true // Automatically stringifies the body to JSON
    };

    rp( options )
    .then( function( data ) {
        console.info( data );
        process.exit( 0 );
    } )
    .catch( ( err ) => {
        console.error( err.message || err );
        process.exit( -1 );
    } );
} )();
