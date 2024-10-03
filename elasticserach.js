const { Client } = require('@elastic/elasticsearch');

const client = new Client({ node: 'http://localhost:9200' }); 


async function createCollection(indexName) {
  try {
    await client.indices.create({ index: indexName });
    console.log(`Index ${indexName} created`);
  } catch (error) {
    console.error('Error creating index ${indexName}:, error');
  }
}

async function getEmpCount(indexName) {
  try {
    const count = await client.count({ index: indexName });
    console.log('Document count in ${indexName}:, count.body.count');
  } catch (error) {
    console.error('Error getting document count for ${indexName}:, error');
  }
}


async function indexData(indexName, data) {
  try {
    await client.index({
      index: indexName,
      document: data,
    });
    console.log('Data indexed in ${indexName}');
  } catch (error) {
    console.error('Error indexing data in ${indexName}:, error');
  }
}


async function delEmpById(indexName, id) {
  try {
    await client.delete({
      index: indexName,
      id: id,
    });
    console.log('Document with ID ${id} deleted from ${indexName}');
  } catch (error) {
    console.error('Error deleting document ${id} from ${indexName}:, error');
  }
}


async function searchByColumn(indexName, field, value) {
  try {
    const result = await client.search({
      index: indexName,
      query: {
        match: { [field]: value },
      },
    });
    console.log('Search results for ${field} = ${value} in ${indexName}:, result.body.hits.hits');
  } catch (error) {
    console.error('Error searching in ${indexName}:, error');
  }
}

async function getDepFacet(indexName) {
  try {
    const result = await client.search({
      index: indexName,
      aggs: {
        departments: {
          terms: { field: 'Department.keyword' }, 
        },
      },
    });
    console.log('Department facets in ${indexName}:, result.body.aggregations.departments.buckets');
  } catch (error) {
    console.error('Error getting department facets in ${indexName}:, error');
  }
}

 
(async () => {
  const v_nameCollection = 'Hash_Jagadeesh';
  const v_phoneCollection = 'Hash_1234';

  await createCollection(v_nameCollection);
  await createCollection(v_phoneCollection);

 
  await indexData(v_nameCollection, { Department: 'IT', Gender: 'Male', EmployeeID: 'E02003' });
  await indexData(v_phoneCollection, { Department: 'HR', Gender: 'Female', EmployeeID: 'E02004' });

  await getEmpCount(v_nameCollection);

  await delEmpById(v_nameCollection, 'E02003');
  
  await getEmpCount(v_nameCollection);

  await searchByColumn(v_nameCollection, 'Department', 'IT');
  await searchByColumn(v_nameCollection, 'Gender', 'Male');
  await searchByColumn(v_phoneCollection, 'Department', 'IT');

  await getDepFacet(v_nameCollection);
  await getDepFacet(v_phoneCollection);
})();