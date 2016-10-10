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
 * @param {any} sites
 * @param {any} acronym
 * @returns
 */
function getPublicAcronym( sites, acronym ) {
    sites = sites.filter( s => s.sigla === acronym );
    return sites[ 0 ].siglaPublica;
}

/**
 *
 *
 * @param {string} site Sigla do site
 * @returns {Promise}
 */
function getAndIndexNews( site, lastId, lastUpdate ) {
    return getLastUpdate( site.sigla, lastUpdate )
        .then( lastUpdate => {
            return orchard().getNews( site.sigla, lastUpdate );
        } )
        .then( news => {
            if ( news.length > 0 ) {
                news = news.map( n => {
                    n.siglaPublica = site.siglaPublica;
                    return n;
                } );

                return elastic().indexNews( news );
            } else {
                return Promise.reject();
            }
        } )
        .then( lastUpdatedNews => {
            elastic().updateLastUpdate( site.sigla, lastUpdatedNews.date );

            if ( lastId !== lastUpdatedNews.id ) {
                return getAndIndexNews( site, lastUpdatedNews.id, lastUpdatedNews.date );
            } else {
                return Promise.reject();
            }
        } )
        .catch( err => {
            if ( !err ) {
                console.log( `Updated all news from ${site.sigla}.` );
                return `Updated all news from ${site.sigla}.`;
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
    _highlights = _highlights.map( n => {
        n.siglaPublica = getPublicAcronym( sites, n.siglaSite );
        return n;
    } );

    let promises = [];
    sites.forEach( site => {
        promises.push( getAndIndexNews( site ) );
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
