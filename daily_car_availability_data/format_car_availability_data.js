const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');

const seed_car_data = [
    {
      count_total_available: '155',
      count_total_on_rent: '1672',
      count_ncm_rp_out: '3',
      count_ncm_dc_out: '16',
      count_other: 37,
      count_total_cars: '1883',
      utilization_total: '88.7945'
    }
];

async function get_formatted_car_avail_data(car_data) {
    const formatted_car_data = car_data.map(data => {
        // Apply Intl.NumberFormat to add commas for numbers directly, without manual conversion to Number()
        const numberFormat = new Intl.NumberFormat();

        // Calculate the utilization as a percentage, rounded to 2 decimal places
        const calculated_utilization_total = (data.count_total_on_rent / data.count_total_cars) * 100;

        return {
            count_total_available: numberFormat.format(data.count_total_available),
            count_total_on_rent: numberFormat.format(data.count_total_on_rent),
            count_ncm_rp_out: numberFormat.format(data.count_ncm_rp_out),
            count_ncm_dc_out: numberFormat.format(data.count_ncm_dc_out),
            count_other: numberFormat.format(data.count_other),
            count_total_cars: numberFormat.format(data.count_total_cars),
            utilization_total: `${calculated_utilization_total.toFixed(2)}%` // rounded to 2 decimals
        };
    });

    return { formatted_car_data };
}


// get_formatted_car_avail_data(seed_car_data);

module.exports = {
    get_formatted_car_avail_data,
};
