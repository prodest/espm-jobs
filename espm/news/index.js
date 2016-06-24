const elasticsearch = require( 'elasticsearch' );
const Promise = require( 'bluebird' );
const request = require( 'request-promise' );

// ElasticSearch
const newsIndex = 'news';
const newsType = 'news';
const lastUpdateIndex = 'news-last-update';
const lastUpdateType = 'news-last-update';
const paralelBlocks = 5;

// Orchard
const orchardApi = process.env.ORCHARD_API || 'http://gtsis2.es.gov.br/api/';
const sitesEndpoint = `${orchardApi}noticias/getsitelist`;
const newsEndpoint = `${orchardApi}noticias/GetNoticiasBySite`;
const maxNews = 50; // Should reflect maximum number of news orchard's API responds

const client = new elasticsearch.Client( {
    host: process.env.ELASTICSEARCH || 'http://10.243.9.4',
    log: 'error'
} );

/**
 * Gets the list of sites available at Orchard
 * @returns {Promise} Http Response
 */
function getOrchardSites() {
    const options = {
        uri: sitesEndpoint,
        headers: {
            'User-Agent': 'Request-Promise'
        },
        json: true
    };

    return request( options );
}

/**
 * Gets last time the news for a site was updated
 * @param {string} site Site acronym
 * @returns {Promise} Promise with last update Date
 */
function getLastUpdate( site ) {

    return client.indices.exists( { index: lastUpdateIndex } )
    .then( existsIndex => {
        if ( !existsIndex ) {
            return client.indices.create( {
                index: lastUpdateIndex
            } );
        } else {
            return Promise.resolve();
        }
    } )
    .then( () => {
        return client.get( {
            index: lastUpdateIndex,
            type: lastUpdateType,
            id: site
        } );
    } )
    .then( lastUpdate => {
        return Promise.resolve( new Date( lastUpdate._source.date ) );
    } )
    .catch( () => {
        return Promise.resolve( new Date( 1900, 1, 1 ) );
    } );
}


/**
 * Get news for a site
 * @param {string} site Site acronym
 * @param {Date} lastUpdate Last time the news was updated
 * @returns {Promise} List of news
 */
function getOrchardNews( site, lastUpdate ) {
    const options = {
        uri: newsEndpoint,
        headers: {
            'User-Agent': 'Request-Promise'
        },
        qs: {
            sitename: site,
            dataUltimaVerificacao: lastUpdate.toISOString().slice( 0, -1 )
        },
        json: true
    };

    return request( options );
}

/**
 * Gets then news from orchard and index with ElasticSearch
 * @param {string} site Site acronym
 * @param {Date} lastUpdate Last time the news was updated
 * @returns {Promise} Bulk response from ElasticSearch or null if nothing was updated
 */
function bulkIndexNews( site, lastUpdate ) {
    return getOrchardNews( site, lastUpdate )
    .then( news => {
        var actions = news.map( n => {
            return {
                index: {
                    _index: newsIndex,
                    _type: newsType,
                    _id: `${n.siglaSite}_${n.noticiaId}`
                }
            };
        } );

        const body = [];
        for ( let i = 0; i < actions.length; i++ ) {
            body.push( actions[ i ] );
            body.push( news[ i ] );
        }

        if ( news.length > 0 ) {
            lastUpdate = new Date( news[ news.length - 1 ].dataModificacao );

            return client.bulk( {
                body: body
            } );
        } else {
            return Promise.resolve();
        }
    } )
    .then( bulk => {
        if ( bulk ) {
            client.index( {
                index: lastUpdateIndex,
                type: lastUpdateType,
                id: site,
                body: {
                    date: lastUpdate
                }
            } );

            if ( bulk.items.length === maxNews ) {
                return bulkIndexNews( site, lastUpdate );
            }
        }

        console.log( `Updated all news from ${site}.` );
        return Promise.resolve( `Updated all news from ${site}.` );
    } );
}

/**
 * Gets last update for a site and index the news with ElasticSearch
 * @param {string} site Site Acronym
 * @returns {Promise} Success message
 */
function getAndIndexNews( site ) {
    return getLastUpdate( site )
    .then( lastUpdate => {
        return bulkIndexNews( site, lastUpdate );
    } );
}

client.indices.exists( {
    index: newsIndex
} )
.then( existsIndex => {
    if ( !existsIndex ) {
        return client.indices.create( {
            index: newsIndex
        } );
    } else {
        return Promise.resolve();
    }
} )
.then( () => {
    return getOrchardSites();
} )
.then( sites => {
    let promises = [];
    sites.forEach( site => {
        promises.push( getAndIndexNews( site.sigla ) );
    } );

    const groupedPromises = [];
    for ( let i = 0; promises.length; i += paralelBlocks ) {
        groupedPromises.push( promises.splice( 0, paralelBlocks ) );
    }

    return Promise.reduce( groupedPromises, ( total, p ) => {
        return Promise.all( p )
            .then( a => {
                return total.concat( a );
            } );
    }, [] );
} )
.then( () => {
    //console.log( a );
    console.log( 'Fim.' );
    process.exit( 0 );
} )
.catch( err => {
    console.log( err );
    process.exit( -1 );
} );
