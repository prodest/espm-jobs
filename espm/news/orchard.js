const request = require( 'request-promise' );

const orchardApi = process.env.ORCHARD_API || 'http://gtsis2.es.gov.br/api/';
const sitesEndpoint = `${orchardApi}noticias/GetSiteList`;
const newsEndpoint = `${orchardApi}noticias/GetNoticiasBySite`;
const highlightsEndpoint = `${orchardApi}noticias/GetDestaques`;

module.exports = () => {

    const orchard = new Object();

    /**
     * Gets the list of sites available at Orchard
     * @returns {Promise} Http Response
     */
    orchard.getSites = function() {
        const options = {
            uri: sitesEndpoint,
            headers: {
                'User-Agent': 'Request-Promise'
            },
            json: true
        };

        return request( options );
    };


    /**
     * Get news for a site
     * @param {string} site Site acronym
     * @param {Date} lastUpdate Last time the news was updated
     * @returns {Promise} List of news
     */
    orchard.getNews = function( site, lastUpdate ) {
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
    };

    orchard.getHighlights = function() {
        const options = {
            uri: highlightsEndpoint,
            headers: {
                'User-Agent': 'Request-Promise'
            },
            json: true
        };

        return request( options );
    };

    return orchard;
};
