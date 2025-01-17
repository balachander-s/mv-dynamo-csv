const AWS = require('aws-sdk');
const { createObjectCsvWriter } = require('csv-writer');


// Parse command-line arguments
const args = process.argv.slice(2);
const companyKey = args[0] || ''; // company key
const profile = args[1] || 'default'; // AWS profile
const region = args[2] || 'us-west-2'; // AWS region


// Set the AWS profile
const credentials = new AWS.SharedIniFileCredentials({ profile }); // aws profile
AWS.config.credentials = credentials;


// Configure AWS SDK
AWS.config.update({ region }); // DynamoDB region

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const tableName = 'qa-live-tocoma-FeedFilter'; // DynamoDB table name
const csvFilePath = 'output.csv'; // File path to write the CSV data


const ViewTableName = 'qa-live-tocoma-ExploreViewColumn'; // Replace with your second table name

const getViewName = async (Key) => {

  const splitKey = Key.split(':');
  const [hashKey, sortKey] = splitKey;
  const params = {
    TableName: ViewTableName,
    Key: {
      hashKey: hashKey,
      rangeKey:  sortKey,
    },
  };

  try {
    const data = await dynamoDB.get(params).promise();
    return data.Item ? data.Item.name : null;
  } catch (error) {
    console.error(`Error querying viewname for hashKey ${hashKey}:`, error.message);
    return null;
  }
};

const flattenMap = (data, parentKey = '', result = {}) => {
  for (const [key, value] of Object.entries(data)) {
    const newKey = parentKey ? `${parentKey}.${key}` : key;

    if (Array.isArray(value)) {
      // Concatenate array values into a single string
      result[newKey] = value.join(', '); // Join array elements with a comma and space
    } else if (value && typeof value === 'object') {
      // Recursively flatten maps
      flattenMap(value, newKey, result);
    } else {
      result[newKey] = value;
    }
  }
  return result;
};

const scanTable = async (params, allItems = []) => {
  const data = await dynamoDB.scan(params).promise();
  allItems = allItems.concat(data.Items);

  if (data.LastEvaluatedKey) {
    params.ExclusiveStartKey = data.LastEvaluatedKey;
    return scanTable(params, allItems);
  }

  return allItems;
};

// Define scan parameters with filters
const scanParams = {
  TableName: tableName,
  FilterExpression: '#companyKey = :companyKeyValue AND (attribute_exists(#filter.#keywordsHashtagsString) OR (attribute_exists(#filter.#keywordsHashtags) AND size(#filter.#keywordsHashtags) > :emptyListSize) OR (attribute_exists(#filter.#quickSearchTerms) AND size(#filter.#quickSearchTerms) > :emptyListSize) OR attribute_exists(#filter.#quickSearchTermsString) OR attribute_exists(#filter.#text))',
  ExpressionAttributeNames: {
    '#companyKey': 'companyKey',
    '#filter': 'filter',
    '#text': 'text',
    '#keywordsHashtagsString': 'keywordsHashtagsString',
    '#keywordsHashtags': 'keywordsHashtags',
    '#quickSearchTerms': 'quickSearchTerms',
    '#quickSearchTermsString': 'quickSearchTermsString',

  },
  ExpressionAttributeValues: {
    ':companyKeyValue': companyKey, // Replace with the actual value you want to filter by
    ':emptyListSize': 0,
  },

};

const allowedKeys = [
  'hashKey',
  'rangeKey',
  'companyKey',
  'filter.keywordsHashtagsString',
  'filter.keywordsHashtags',
  'filter.quickSearchTerms',
  'filter.quickSearchTermsString',
  'filter.text',
  'viewname',
];

const exportToCSV = async () => {
  try {
    console.log('Scanning DynamoDB table...');
    const items = await scanTable(scanParams);

    console.log(`Fetched ${items.length} items from DynamoDB.`);
    if (items.length === 0) {
      console.log('No data to export.');
      return;
    }

    console.log('Flattening data...');
    const flattenedItems = await Promise.all(items.map(async item => {
      console.log('Fetching viewnames from ExploreViewColumn table...' + item.hashKey);
      item.viewname = await getViewName(item.hashKey);
      console.log('Viewname:', item.viewname);
      return flattenMap(item);
    }));

    // Collect all unique column names from flattened items
    const headers = Array.from(
      new Set(flattenedItems.flatMap(item => Object.keys(item)))
    )
    .filter(key => allowedKeys.some(allowedKey => key.includes(allowedKey)))
    .map(key => ({ id: key, title: key }));

    console.log('Writing data to CSV...');
    const csvWriter = createObjectCsvWriter({
      path: csvFilePath,
      header: headers,
    });

    await csvWriter.writeRecords(flattenedItems);
    console.log(`Data successfully written to ${csvFilePath}`);
  } catch (error) {
    console.error('Error exporting data:', error.message);
  }
};

exportToCSV();
