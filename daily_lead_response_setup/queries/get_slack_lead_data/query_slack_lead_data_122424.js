const { getPakistanTime } = require('../../../utilities/getCurrentDate');

async function query_lead_metrics_data(country, date) {

    // if date is null then today in pst
    const pakistan_date = await getPakistanTime();
    date = date ? date : pakistan_date;
    console.log('\npakistan date ', pakistan_date);

    // if country is null then all
    country = country ? country : null;
    console.log('query lead data =', country, country === null, date, '.\n');

    return `
        SELECT 
            *
        FROM lead_response_metrics_data
        WHERE 
			created_on_pst = '${date}'
            AND
            -- if country IS NULL evalutes to true then returns all countries
            -- else return renting_in_country = the passed country
            (${country === null} OR renting_in_country_abb = '${country}')

        ORDER BY created_on_pst DESC
    `;
}

module.exports = {
    query_lead_metrics_data,
}