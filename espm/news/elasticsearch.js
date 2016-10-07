const Promise = require( 'bluebird' );
const elasticsearch = require( 'elasticsearch' );

// ElasticSearch
const newsIndex = 'news';
const newsType = 'news';
const highlightsIndex = 'highlights';
const highlightsType = 'highlights';
const lastUpdateIndex = 'news-last-update';
const lastUpdateType = 'news-last-update';

const client = new elasticsearch.Client( {
    host: process.env.ELASTICSEARCH || 'http://10.243.9.4',
    log: 'error'
} );

module.exports = () => {

    const elastic = new Object();

    /**
     *
     *
     * @param {any} indexName
     * @returns {Promise}
     */
    function createIndexIfNotExists( indexName ) {
        return client.indices.exists( {
            index: indexName
        } )
        .then( existsIndex => {
            if ( !existsIndex ) {
                return client.indices.create(
                    {
                        index: indexName
                    } );
            } else {
                return Promise.resolve();
            }
        } );
    }

    /**
     *
     *
     * @param {any} indexName
     * @param {any} alias
     * @returns
     */
    function setAlias( indexName, alias ) {
        return client.indices.putAlias( {
            index: indexName,
            name: alias
        } );
    }

    /**
     *
     *
     * @returns {Promise} Empty promise
     */
    elastic.createIndexesIfNotExists = function( ) {
        return createIndexIfNotExists( newsIndex )
        .then( () => {
            return createIndexIfNotExists( lastUpdateIndex );
        } )
        .then( () => Promise.resolve() );
    };

    /**
     * Gets last time the news for a site was updated
     * @param {string} site Site acronym
     * @returns {Promise} Promise with last update Date
     */
    elastic.getLastUpdate = function( site ) {
        return client.get(
            {
                index: lastUpdateIndex,
                type: lastUpdateType,
                id: site
            } )
            .then( getResponse => {
                return Promise.resolve( new Date( getResponse._source.date ) );
            } )
            .catch( () => {
                return Promise.resolve( new Date( 1900, 1, 1 ) );
            } );
    };

    /**
     *
     *
     * @param {any} news
     * @returns
     */
    elastic.indexNews = function( news ) {

        const actions = news.map( n => {
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

        const lastNews = news[ news.length - 1 ];
        const lastUpdate = new Date( lastNews.dataModificacao );

        return client.bulk( {
            body: body
        } )
        .then( () => {
            return lastUpdate;
        } );
    };


    elastic.indexHighlights = function( highlights ) {
        if ( highlights.length == 0 ) {
            return Promise.reject( 'No highlights found.' );
        }

        const tempIndexName = 'highlights_'
                + new Date().toISOString().slice( 0, 19 ).toLowerCase();

        const actions = highlights.map( n => {
            return {
                index: {
                    _index: tempIndexName,
                    _type: highlightsType,
                    _id: `${n.siglaSite}_${n.noticiaId}`
                }
            };
        } );

        const body = [];
        for ( let i = 0; i < highlights.length; i++ ) {
            body.push( actions[ i ] );
            body.push( highlights[ i ] );
        }

        return client.bulk( {
            body: body
        } )
        .then( () => tempIndexName );
    };

    /**
     *
     *
     * @param {any} site
     * @param {any} lastUpdate
     * @returns
     */
    elastic.updateLastUpdate = function( site, lastUpdate ) {
        return client.index( {
            index: lastUpdateIndex,
            type: lastUpdateType,
            id: site,
            body: {
                date: lastUpdate
            }
        } );
    };

    elastic.setHighlightsAlias = function( indexName ) {
        return elastic.removeIndex( highlightsIndex )
        .then( () => setAlias( indexName, highlightsIndex ) );
    };

    elastic.removeIndex = function( indexName ) {
        return client.indices[ 'delete' ]( {
            index: indexName
        } );
    };

    return elastic;
};

