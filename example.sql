INSERT INTO rdfuser.net1#rdft_people(triple) VALUES (
  SDO_RDF_TRIPLE_S(
    'people',
    '<http://example.com/person/Alice>',
    '<http://example.com/ontology/worksAt>',
    '<http://example.com/company/OpenAI>',
    network_owner => 'RDFUSER',
    network_name  => 'NET1'
  )
);

INSERT INTO rdfuser.net1#rdft_people(triple) VALUES (
  SDO_RDF_TRIPLE_S(
    'people',
    '<http://example.com/person/Alice>',
    '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
    '<http://example.com/ontology/Person>',
    network_owner => 'RDFUSER',
    network_name  => 'NET1'
  )
);

INSERT INTO rdfuser.net1#rdft_people(triple) VALUES (
  SDO_RDF_TRIPLE_S(
    'people',
    '<http://example.com/company/OpenAI>',
    '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
    '<http://example.com/ontology/Company>',
    network_owner => 'RDFUSER',
    network_name  => 'NET1'
  )
);