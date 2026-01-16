// @ts-nocheck
const { parse } = require('csv-parse/sync')
const fs = require('fs')
const path = require('path')

module.exports = (() => {
  const records = parse(fs.readFileSync(path.join(__dirname, 'property-websites.csv'), 'utf8'), {
    columns: true,
    skip_empty_lines: true
  })

  const spidWebsiteMap = new Map()

  records.forEach(columns => {
    const { siteminder_property_id: spid, home_page_link: website } = columns
    const propertyWebsite = website.trim() !== 'null' && website.trim() !== '' ? website.trim() : null
    spidWebsiteMap.set(spid, propertyWebsite)
  })

  return spidWebsiteMap
})();

// Databricks query for reference
// https://siteminder-pciprod.cloud.databricks.com/editor/queries/4166014845012662?o=7316601961078759
/*
select region,
       id,
       replace(channel_code, '\n', '\\n') as channel_code,
       siteminder_property_id,
       replace(name, '\n', '\\n') as name,
       partner, group, payment_gateway_name,
       replace(home_page_link, '\n', '\\n') as home_page_link,
       custom_domain,
       deleted,
       suspended
FROM (
  SELECT 'apac' region, p.siteminder_property_id, w.name as partner, g.name as group, b.channel_code, p.*
  FROM pciprod_prod_views.`tbb_core-apac`.properties p
  LEFT JOIN pciprod_prod_views.`tbb_core-apac`.groups g ON g.id = p.group_id
  LEFT JOIN pciprod_prod_views.`tbb_core-apac`.partners w ON w.id = g.partner_id
  LEFT JOIN pciprod_prod_views.`tbb_core-apac`.booking_channels b ON b.property_id = p.id
  UNION ALL
  SELECT 'emea' region, p.siteminder_property_id, w.name as partner, g.name as group, b.channel_code, p.*
  FROM pciprod_prod_views.`tbb_core-emea`.properties p
  LEFT JOIN pciprod_prod_views.`tbb_core-emea`.groups g ON g.id = p.group_id
  LEFT JOIN pciprod_prod_views.`tbb_core-emea`.partners w ON w.id = g.partner_id
  LEFT JOIN pciprod_prod_views.`tbb_core-emea`.booking_channels b ON b.property_id = p.id
) merged_properties
where siteminder_property_id IN (
 ...list of SPIDs...
);
*/
