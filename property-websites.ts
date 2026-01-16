import { parse } from 'csv-parse/sync'
import * as fs from 'fs'
import * as path from 'path'

interface PropertyRecord {
  siteminder_property_id: string
  home_page_link: string
}

const propertyWebsites: Map<string, string | null> = (() => {
  const records = parse(fs.readFileSync(path.join(__dirname, 'property-websites.csv'), 'utf8'), {
    columns: true,
    skip_empty_lines: true
  }) as PropertyRecord[]

  const spidWebsiteMap = new Map<string, string | null>()

  records.forEach(columns => {
    const { siteminder_property_id: spid, home_page_link: website } = columns
    const propertyWebsite = website.trim() !== 'null' && website.trim() !== '' ? website.trim() : null
    spidWebsiteMap.set(spid, propertyWebsite)
  })

  return spidWebsiteMap
})()

export default propertyWebsites

// Databricks query for reference
// https://siteminder-pciprod.cloud.databricks.com/editor/queries/4166014845012662?o=7316601961078759
/*
select region,
       id,
       replace(channel_code, '\n', '\\n') as channel_code,
       siteminder_property_id,
       replace(name, '\n', '\\n') as name,
       replace(property_code, '\n', '\\n') as property_code,
       channel_type,
       replace(channel_name, '\n', '\\n') as channel_name,
       home_page_link,
       replace(address_link, '\n', '\\n') as address_link,
       replace(email_link, '\n', '\\n') as email_link
       from product_catalog.properties.enriched_channels
where channel_type = 'Website' and status = 'Active'
order by region, id
*/
