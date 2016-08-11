const Promise = require( 'bluebird' );
const elastic = require( './elasticsearch' );
const orchard = require( './orchard' );

const _paralelBlocks = 5;

/**
 *
 *
 * @param {string} site Sigla do site
 * @returns {Promise}
 */
function getAndIndexNews( site ) {
    return elastic().getLastUpdate( site )
        .then( lastUpdate => {
            return orchard().getNews( site, lastUpdate );
        } )
        .then( news => {
            if ( news.length > 0 ) {
                return elastic().indexNews( news );
            } else {
                return Promise.reject( );
            }
        } )
        .then( lastUpdate => {
            return elastic().updateLastUpdate( site, lastUpdate );
        } )
        .then( () => {
            return getAndIndexNews( site );
        } )
        .catch( err => {
            if ( !err ) {
                console.log( `Updated all news from ${site}.` );
                return `Updated all news from ${site}.`;
            } else {
                throw err;
            }
        } );
}

let _highlights;
elastic().createIndexesIfNotExists()
.then( () => orchard().getHighlights() )
.then( ( highlights ) => {
    _highlights = highlights;
    return orchard().getSites();
} )
.then( sites => {
    let promises = [];
    sites.forEach( site => {
        promises.push( getAndIndexNews( site.sigla ) );
    } );

    const groupedPromises = [];
    for ( let i = 0; promises.length; i += _paralelBlocks ) {
        groupedPromises.push( promises.splice( 0, _paralelBlocks ) );
    }

    return Promise.reduce( groupedPromises, ( total, p ) => {
        return Promise.all( p )
            .then( a => {
                return total.concat( a );
            } );
    }, [] );
} )
.then( () => elastic().indexHighlights( _highlights ) )
.then( () => {
    console.log( 'Fim.' );
    process.exit( 0 );
} )
.catch( err => {
    console.log( err );
    process.exit( -1 );
} );
