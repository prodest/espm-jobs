const Promise = require( 'bluebird' );
const elastic = require( './elasticsearch' );
const orchard = require( './orchard' );

const _parallelBlocks = process.env.PARALLEL_BLOCKS || 1;

/**
 * 
 * 
 * @param {any} site
 * @param {any} lastUpdate
 * @returns
 */
function getLastUpdate( site, lastUpdate ) {
    if ( lastUpdate ) {
        return Promise.resolve( lastUpdate );
    } else {
        return elastic().getLastUpdate( site );
    }
}

/**
 *
 *
 * @param {string} site Sigla do site
 * @returns {Promise}
 */
function getAndIndexNews( site, lastId, lastUpdate ) {
    return getLastUpdate( site, lastUpdate )
        .then( lastUpdate => {
            return orchard().getNews( site, lastUpdate );
        } )
        .then( news => {
            if ( news.length > 0 ) {
                return elastic().indexNews( news );
            } else {
                return Promise.reject();
            }
        } )
        .then( lastUpdatedNews => {
            elastic().updateLastUpdate( site, lastUpdatedNews.date );

            if ( lastId !== lastUpdatedNews.id ) {
                return getAndIndexNews( site, lastUpdatedNews.id, lastUpdatedNews.date );
            } else {
                return Promise.reject();
            }
        } )
        // .then( ( lastUpdatedNews ) => {
        //     if ( lastId !== lastUpdatedNews.id ) {
        //         return getAndIndexNews( site, lastUpdatedNews.id, lastUpdatedNews.date );
        //     } else {
        //         return Promise.reject();
        //     }
        // } )
        .catch( err => {
            if ( !err ) {
                console.log( `Updated all news from ${site}.` );
                return `Updated all news from ${site}.`;
            } else {
                throw err;
            }
        } );
}

let _highlights = '';
elastic().createIndexesIfNotExists()
.then( () => orchard().getHighlights() )
.then( ( highlights ) => {
    _highlights = highlights;
    return orchard().getSites();
} )
.then( sites => {
    let promises = [];
    sites.forEach( site => {
        if ( site.sigla === site.siglaPublica ) {
            promises.push( getAndIndexNews( site.sigla ) );
        }
    } );

    const groupedPromises = [];
    for ( let i = 0; promises.length; i += _parallelBlocks ) {
        groupedPromises.push( promises.splice( 0, _parallelBlocks ) );
    }

    return Promise.reduce( groupedPromises, ( total, p ) => {
        return Promise.all( p )
            .then( a => {
                return total.concat( a );
            } );
    }, [] );
} )
.then( () => elastic().indexHighlights( _highlights ) )
.then( ( highlightsTempIndexName ) => elastic().setHighlightsAlias( highlightsTempIndexName ) )
.then( () => {
    console.log( 'Updated highlights.' );
    console.log( 'Fim.' );
    process.exit( 0 );
} )
.catch( err => {
    console.log( err );
    process.exit( -1 );
} );
