# Trim table based on PK statistics

This exercise is to create a simple script to trim a table in parallel based on the statistics histogram.

## Table with (UUID) PK

```sql
create table u (
    id UUID DEFAULT gen_random_uuid() primary key, 
    ts TIMESTAMP default now(),
    delboolean BOOLEAN DEFAULT IF (random()<0.05,'true','false'),
    s STRING DEFAULT lpad('Z',256,'A')
);

insert into u (id) select gen_random_uuid() from generate_series(1,100000);


root@localhost:26259/defaultdb> select delboolean, count(*) from u group by 1;
  delboolean | count
-------------+--------
    false    | 95064
     true    |  4936

alter table u split at select '10000000-0000-0000-0000-000000000000';
alter table u split at select '20000000-0000-0000-0000-000000000000';
alter table u split at select '30000000-0000-0000-0000-000000000000';
alter table u split at select '40000000-0000-0000-0000-000000000000';
alter table u split at select '50000000-0000-0000-0000-000000000000';
alter table u split at select '60000000-0000-0000-0000-000000000000';
alter table u split at select '70000000-0000-0000-0000-000000000000';
alter table u split at select '80000000-0000-0000-0000-000000000000';
alter table u split at select '90000000-0000-0000-0000-000000000000';
alter table u split at select 'a0000000-0000-0000-0000-000000000000';
alter table u split at select 'b0000000-0000-0000-0000-000000000000';
alter table u split at select 'c0000000-0000-0000-0000-000000000000';
alter table u split at select 'd0000000-0000-0000-0000-000000000000';
alter table u split at select 'e0000000-0000-0000-0000-000000000000';
alter table u split at select 'f0000000-0000-0000-0000-000000000000';

```

```sql
analyze u;

root@localhost:26259/defaultdb> show statistics for table u;
  statistics_name | column_names |          created           | row_count | distinct_count | null_count |    histogram_id
------------------+--------------+----------------------------+-----------+----------------+------------+---------------------
  __auto__        | {id}         | 2021-09-20 23:51:09.943695 |    100000 |         101169 |          0 | 695046864748118018
  __auto__        | {ts}         | 2021-09-20 23:51:09.943695 |    100000 |              1 |          0 | 695046865054990338
  __auto__        | {delboolean} | 2021-09-20 23:51:09.943695 |    100000 |              2 |          0 | 695046865316610050
  __auto__        | {s}          | 2021-09-20 23:51:09.943695 |    100000 |              1 |          0 | 695046865464688642
(4 rows)

Time: 6ms total (execution 6ms / network 0ms)

root@localhost:26259/defaultdb> show histogram 695046864748118018;
               upper_bound               | range_rows | distinct_range_rows | equal_rows
-----------------------------------------+------------+---------------------+-------------
  '0001ee1b-62ae-4d4b-9e12-ef0c471222d8' |          0 |                   0 |         10
  '014ad878-ec1c-4fe1-88d5-8caf2b362d23' |        490 |             504.845 |         10
  '02837889-1f80-4ee8-9708-17f0b3ede0e0' |        490 |             504.845 |         10
  '039647a7-9f83-48c4-a5cb-802881ef9eac' |        490 |             504.845 |         10
  '04ba6e77-8322-472f-8c0c-668815904cf8' |        490 |             504.845 |         10
  '05b28217-6374-4378-bd43-421bef849d8e' |        490 |             504.845 |         10
  '06d08d4a-8f1e-4f18-b9a8-d9cc01a8ecd1' |        490 |             504.845 |         10
  '07ec15d5-3f51-469e-908d-153207af7c70' |        490 |             504.845 |         10
  '09542dd6-a038-436c-9ec2-55f31752522c' |        490 |             504.845 |         10
  '0aaa22ca-7935-4506-a801-d3035466e564' |        490 |             504.845 |         10
  '0bd1c56c-c941-47e6-8388-f32b5498bd2f' |        490 |             504.845 |         10
  '0cc4bf41-1f77-4861-ae5b-5fa91078abfa' |        490 |             504.845 |         10
  '0e79e489-7e67-42d9-8ce6-61547758579d' |        490 |             504.845 |         10
  '0f970f1c-3a5f-4b9a-82bc-cd6fefb2d102' |        490 |             504.845 |         10
  '10ce30d1-3675-4d07-8696-469f85430c91' |        490 |             504.845 |         10
  '11b7245a-30aa-442c-9a7d-9c8dff10e0ae' |        490 |             504.845 |         10
  '12ce0479-110f-4f59-9362-3af6aefb95f8' |        490 |             504.845 |         10
  '13e54402-72b0-487b-8944-897ad8ad13ba' |        490 |             504.845 |         10
  '15c449c3-a063-4c42-b744-4b2a9ce77599' |        490 |             504.845 |         10
  '17317b51-0d47-4412-9f38-78f8d47a83d6' |        490 |             504.845 |         10
  '18c4a41f-1160-48c8-94fa-18ee57fb9d52' |        490 |             504.845 |         10
  '1a175de5-a4ad-461e-9a7b-8140707eebd2' |        490 |             504.845 |         10
  '1b617847-1763-44e0-8129-5adb0a53d254' |        490 |             504.845 |         10
  '1ca8827a-92cf-474c-9570-d9c42a1ba88e' |        490 |             504.845 |         10
  '1e05cb5e-f0df-4c28-87c6-c8a232469802' |        490 |             504.845 |         10
  '1f06085a-4257-4c7f-8fde-d7762d5a97e8' |        490 |             504.845 |         10
  '2056ec58-dbab-49bd-8600-0ea59a858c98' |        490 |             504.845 |         10
  '2194f747-2280-4c09-97e6-17ea71803940' |        490 |             504.845 |         10
  '22ce028d-8ebe-45e5-b5ce-8892a30640f0' |        490 |             504.845 |         10
  '245cbb44-5ffe-4f53-a4e8-453830bebbde' |        490 |             504.845 |         10
  '258c8755-fab8-4534-9ee2-3f8f2bd22101' |        490 |             504.845 |         10
  '26cdc2de-ab88-489e-8354-63a58eeb78d1' |        490 |             504.845 |         10
  '27eb5aff-3c9d-435d-819c-8cf28ee132e2' |        490 |             504.845 |         10
  '28d4d5a5-168f-4091-95e5-d28c1f98c178' |        490 |             504.845 |         10
  '29bd9912-7ce1-43c3-b5bf-89f00a1979cf' |        490 |             504.845 |         10
  '2ad11ff1-9739-42f8-9c49-f96679b1b252' |        490 |             504.845 |         10
  '2c0a77ac-221c-4f90-8c0e-adbf9e73b64e' |        490 |             504.845 |         10
  '2d2814a3-1bdf-44fa-961b-ea22b9389303' |        490 |             504.845 |         10
  '2e9627a8-a7f6-4913-90a9-f64200154189' |        490 |             504.845 |         10
  '2fd79818-d957-472e-883b-559205a1a5b2' |        490 |             504.845 |         10
  '31d9f70c-0aa8-4199-a9bc-82fd552ee7a6' |        490 |             504.845 |         10
  '331e4996-247d-450a-bc74-255fcb5889c4' |        490 |             504.845 |         10
  '34943c4e-eaca-44e3-853a-05781d41e1b0' |        490 |             504.845 |         10
  '35b710cb-90f7-42d0-86cc-c342f865d09f' |        490 |             504.845 |         10
  '36e5a829-9ddc-4bd0-8536-b6513fec75f4' |        490 |             504.845 |         10
  '38365fda-d09a-4b6a-b1f1-d9c354733f64' |        490 |             504.845 |         10
  '39ccf3ac-99f9-43a0-8710-2e06dd01a520' |        490 |             504.845 |         10
  '3ae314fb-1137-4d87-a77b-e5836339a929' |        490 |             504.845 |         10
  '3c326192-8628-4c02-b9b9-3a9b63c7aaf8' |        490 |             504.845 |         10
  '3d37144d-934e-4df9-8d30-fc65a241b3ad' |        490 |             504.845 |         10
  '3ee7900d-615e-4d55-8ab3-99fd0b2afc95' |        490 |             504.845 |         10
  '405b5eb9-0724-4f97-a6d6-6d5fa923ee7d' |        490 |             504.845 |         10
  '417e672f-2037-402c-b889-d71bcb035bd9' |        490 |             504.845 |         10
  '42c82efa-e653-47ea-a3a0-dc298569625b' |        490 |             504.845 |         10
  '43c1af6b-80d5-4899-a2b8-226b2262a598' |        490 |             504.845 |         10
  '44f4bb32-9cce-4633-a436-536a7841ae1c' |        490 |             504.845 |         10
  '4640a9ad-33ef-4de1-bb48-2d0edfb18834' |        490 |             504.845 |         10
  '47666f50-6a11-482d-936e-4af51ea41b67' |        490 |             504.845 |         10
  '486c6141-0777-45e5-8ff2-ea7dc89d4699' |        490 |             504.845 |         10
  '49a92782-0447-42b2-8048-9f6277555959' |        490 |             504.845 |         10
  '4afa6924-2412-4998-9248-c06f5b9ecc46' |        490 |             504.845 |         10
  '4c7d89ff-56a9-41bf-84d3-7b6cba9af46f' |        490 |             504.845 |         10
  '4da4e278-93a0-4ba0-977a-e8d0b0f9ca3c' |        490 |             504.845 |         10
  '4f101110-9205-4564-a462-0bbd49223d48' |        490 |             504.845 |         10
  '50051529-7c0a-4af6-9fdd-bf12183956a0' |        490 |             504.845 |         10
  '513a7891-1f57-4023-b062-1b2dfecb36c9' |        490 |             504.845 |         10
  '52756e22-71f8-45fa-b05b-e6832222a957' |        490 |             504.845 |         10
  '53b7712f-183f-4e92-9a3f-d3d19e41b188' |        490 |             504.845 |         10
  '54fff30f-8850-4a2e-9767-ffe0513950f3' |        490 |             504.845 |         10
  '5643b456-9016-4d4c-873e-5c96ceee1a8c' |        490 |             504.845 |         10
  '57a11382-efed-4a1b-acc4-669265a06841' |        490 |             504.845 |         10
  '58d67820-cf48-40f8-a31c-b18cf16948a3' |        490 |             504.845 |         10
  '5a214e35-5c05-4eb8-9cf7-b130b7f30b96' |        490 |             504.845 |         10
  '5b554545-dafb-4fe7-8eef-375235d84380' |        490 |             504.845 |         10
  '5c702b13-c8bc-423c-b5ec-f93e54a3f6b1' |        490 |             504.845 |         10
  '5da3c88b-fd27-49e2-ba6d-17ce1f9cd71f' |        490 |             504.845 |         10
  '5f0744c3-5dc7-46a8-8dae-3dc0218a549c' |        490 |             504.845 |         10
  '6066cf0c-0de5-456d-a85a-49fa1d7cce42' |        490 |             504.845 |         10
  '61d614ec-b321-4a3a-b7e6-e1c0da9be8a2' |        490 |             504.845 |         10
  '62ce173e-56eb-4728-8af0-9ef42cf83ad4' |        490 |             504.845 |         10
  '63c6d46e-6cbc-457a-9eb8-d4b5e6d2356e' |        490 |             504.845 |         10
  '64d6e781-2b83-4d6d-bc80-ba8a4e9a7739' |        490 |             504.845 |         10
  '66031cf1-f9af-4012-a087-5282b002a28a' |        490 |             504.845 |         10
  '6735d4d4-bd19-44d6-8dbc-76f4688a0165' |        490 |             504.845 |         10
  '687ffb52-2bc3-4bbb-9d2d-69f6571804d0' |        490 |             504.845 |         10
  '69c9ac84-00f1-420c-9b05-5668403647ac' |        490 |             504.845 |         10
  '6af53679-9c3f-4c2b-b87b-ad9014db99c0' |        490 |             504.845 |         10
  '6c719659-148d-41de-aaeb-db1ab930877d' |        490 |             504.845 |         10
  '6db450a8-d9ca-49de-86af-3a43693984fe' |        490 |             504.845 |         10
  '6ee9eb3a-4875-47b2-9d20-b00109aabe25' |        490 |             504.845 |         10
  '70185ea0-4267-4e37-8434-ab21d91fedec' |        490 |             504.845 |         10
  '71b729e2-a43f-49a2-bcea-e2f466902c4b' |        490 |             504.845 |         10
  '72e41d56-ba1a-4df1-a1c0-f497b36c19f5' |        490 |             504.845 |         10
  '74148691-c6e4-44dd-bed9-da6b1a71ef78' |        490 |             504.845 |         10
  '754f5f4e-fbcf-412c-908e-214aa9f944e7' |        490 |             504.845 |         10
  '76ae6b9f-2b28-4e13-8e37-9249b3708871' |        490 |             504.845 |         10
  '78149654-9f76-478f-97a8-fd51980bd4b8' |        490 |             504.845 |         10
  '79555a0b-7b28-4d97-98bd-c60b65e7d9a3' |        490 |             504.845 |         10
  '7a975a12-0f91-4d52-97c5-6a4abfbc563c' |        490 |             504.845 |         10
  '7bc606f8-a56d-43b3-92dd-1c3ffc26f0a1' |        490 |             504.845 |         10
  '7d63af97-f967-4140-b2ac-0cbd9a25111a' |        490 |             504.845 |         10
  '7ebeda7e-0643-4e81-8254-732fc06bb88d' |        490 |             504.845 |         10
  '801679b7-12c1-4467-90b3-b09b0315da00' |        490 |             504.845 |         10
  '813573ca-4610-491b-ac7c-a64a4c2c1ca0' |        490 |             504.845 |         10
  '8287325a-104d-4e44-92a4-36280c92cfcc' |        490 |             504.845 |         10
  '83e7ce2c-f3a6-46e2-85de-e802844803b3' |        490 |             504.845 |         10
  '85476ce7-0cfd-4a63-84f7-d9d52ce7d649' |        490 |             504.845 |         10
  '86a98837-2f98-4ada-92eb-668689f84966' |        490 |             504.845 |         10
  '880234e2-fdd9-4aa3-a0e6-0f824ba16085' |        490 |             504.845 |         10
  '892cee13-a711-4e10-b29c-876b74378fca' |        490 |             504.845 |         10
  '8aa888b1-9c4e-434b-81f4-24e27a8f58c2' |        490 |             504.845 |         10
  '8bc996c5-d1bd-4c93-8c4e-d6b3ae2a8756' |        490 |             504.845 |         10
  '8ce3f5e6-eab2-4634-9d4d-c36aa121fbd4' |        490 |             504.845 |         10
  '8e3a8a93-567d-41a5-aba3-33d43f8d8a19' |        490 |             504.845 |         10
  '8fff31ca-e2bd-4b5b-b5f3-8f1e3a9f8c5d' |        490 |             504.845 |         10
  '91492f68-65c9-4fd6-94b8-648196ab1458' |        490 |             504.845 |         10
  '92d27d78-15a8-4334-94d3-a8d9ae4a9f7f' |        490 |             504.845 |         10
  '93fdff72-08c5-48e2-a82c-e2b1ae31cb06' |        490 |             504.845 |         10
  '953ce043-944e-4159-837c-79a0e4688b97' |        490 |             504.845 |         10
  '96c6a490-79fb-4c1a-9c38-42d0a647a80f' |        490 |             504.845 |         10
  '9813813d-9fec-4b73-97f9-16bddc41b8fb' |        490 |             504.845 |         10
  '995b0c87-a58f-4cde-8492-6b3fba42a8a4' |        490 |             504.845 |         10
  '9a3ec90e-8e9d-4f0c-b67c-cb0f14f26222' |        490 |             504.845 |         10
  '9b5196bc-6f37-4bb8-90f1-3f6f9c5a2591' |        490 |             504.845 |         10
  '9ce1cddb-7b99-4638-af83-9274eaec7dbe' |        490 |             504.845 |         10
  '9e203683-4f7f-4409-9262-7353e3fa2eb7' |        490 |             504.845 |         10
  '9f6fe0eb-0f47-41a3-9d2c-8af60da33353' |        490 |             504.845 |         10
  'a0de2629-a522-430a-9fbf-be9f8f907cbe' |        490 |             504.845 |         10
  'a1f7d094-12a0-48ea-ae94-b4c0a4c95cf5' |        490 |             504.845 |         10
  'a30f48a0-2520-45ae-949d-a140eea5a2c9' |        490 |             504.845 |         10
  'a42a96d6-7310-4a29-abeb-c4cdc5dad61c' |        490 |             504.845 |         10
  'a57f8e89-81a0-4859-bf82-a638e1753a21' |        490 |             504.845 |         10
  'a71709e8-6bbc-47df-a7c4-ef49098191f0' |        490 |             504.845 |         10
  'a876df9d-aa6a-47c8-a9f6-f51058f44d22' |        490 |             504.845 |         10
  'a9efeeb5-45bc-4ab7-96c9-ac18f8bb6b60' |        490 |             504.845 |         10
  'ab591470-78c8-42d0-a7a0-4d4516962e57' |        490 |             504.845 |         10
  'acecc81d-4b68-4f4c-8e48-8e5ce34596a0' |        490 |             504.845 |         10
  'ae6d6a26-fb1d-41f2-b1b8-f0b2a70236cc' |        490 |             504.845 |         10
  'afae3292-2cb5-43d2-aee5-75db2143d84b' |        490 |             504.845 |         10
  'b118fa4e-06d9-426a-bfe3-e6bf66d15ca9' |        490 |             504.845 |         10
  'b26585bd-5a2a-48e0-abee-15ea71adb02e' |        490 |             504.845 |         10
  'b39e6705-9772-4bb6-9a87-67f39bde64cd' |        490 |             504.845 |         10
  'b4d99535-88ae-4d05-b18f-5e32478122a2' |        490 |             504.845 |         10
  'b65fb3aa-5d5a-43fe-9e50-cdb1e8067029' |        490 |             504.845 |         10
  'b775a843-c639-46bc-97e5-871d0c312771' |        490 |             504.845 |         10
  'b8839659-2629-403d-8407-dd17e6ab9351' |        490 |             504.845 |         10
  'b9cfb186-8eb4-4e7e-adfc-f4f323f42172' |        490 |             504.845 |         10
  'bb1ad071-c986-4333-b5a9-818006ad6bac' |        490 |             504.845 |         10
  'bc600d72-a676-40bb-82a6-8b78658815c8' |        490 |             504.845 |         10
  'be0ece1e-68ba-4c68-9e02-8f99bbc3ce74' |        490 |             504.845 |         10
  'bf322669-f8f1-4bfb-b3cc-994efe8ba951' |        490 |             504.845 |         10
  'c05cfa21-c666-4fe2-907a-baf5415ccf6e' |        500 |   515.1479591836735 |         10
  'c1cef25c-24b7-4fdc-9844-2158b73c14b6' |        500 |   515.1479591836735 |         10
  'c331f820-8e72-48ee-bc89-e3b2a3776c88' |        500 |   515.1479591836735 |         10
  'c463dfde-cac8-450d-a518-bdd295643c32' |        500 |   515.1479591836735 |         10
  'c5f3b96d-17e1-4c23-9976-3fa3896e0821' |        500 |   515.1479591836735 |         10
  'c72a6ad6-b4dd-42a8-bbf3-5fa7996b1c01' |        500 |   515.1479591836735 |         10
  'c8297346-791e-4cff-a69d-4e981e5aa060' |        500 |   515.1479591836735 |         10
  'c95fa8a0-437a-4b22-8007-832cd48ce802' |        500 |   515.1479591836735 |         10
  'caa5240e-9c5e-4f98-ac08-dd2c22b62591' |        500 |   515.1479591836735 |         10
  'cbe8c05b-884b-4afb-806e-ea695eda455a' |        500 |   515.1479591836735 |         10
  'cd58f4d3-240e-4b52-a2ae-10a4992c781e' |        500 |   515.1479591836735 |         10
  'cf017c2a-d2e9-4853-91e6-069b7258b735' |        500 |   515.1479591836735 |         10
  'd03d6019-13a6-41c9-a707-7593264dd7df' |        500 |   515.1479591836735 |         10
  'd1976374-2861-4346-a6d3-10b0eec29e22' |        500 |   515.1479591836735 |         10
  'd2f3df6a-aaf9-4cc0-989c-49b886d0e6b9' |        500 |   515.1479591836735 |         10
  'd45612b3-6c59-4cd4-aebc-9793464a5995' |        500 |   515.1479591836735 |         10
  'd5b5df8a-0949-4cef-a93e-643e5ed20167' |        500 |   515.1479591836735 |         10
  'd76e271e-e217-4ead-b8f2-1d145317f74c' |        500 |   515.1479591836735 |         10
  'd8a4f59d-35ed-4a39-bb31-4895c3e2a3bc' |        500 |   515.1479591836735 |         10
  'da02990f-4f32-4953-8665-769b601ccaee' |        500 |   515.1479591836735 |         10
  'db53299d-2037-4346-b251-383dc3834a54' |        500 |   515.1479591836735 |         10
  'dc9bdc18-0577-4856-ba8d-207a6073ebba' |        500 |   515.1479591836735 |         10
  'dd9f3c38-93dd-41f9-89d7-f4a549452d56' |        500 |   515.1479591836735 |         10
  'de8b7469-60ba-4e30-a9ae-68b9bfed40f1' |        500 |   515.1479591836735 |         10
  'dff877c9-adc7-4b78-a758-324f1ccaa441' |        500 |   515.1479591836735 |         10
  'e12405d5-963d-4463-8ae3-51eb0d44a294' |        500 |   515.1479591836735 |         10
  'e24c989d-6b2a-461f-95f3-1368c6c17c59' |        500 |   515.1479591836735 |         10
  'e3b352cd-1b29-49df-80f2-a6f4ab0f822d' |        500 |   515.1479591836735 |         10
  'e4fd8efc-0a56-4f52-a955-b5dc29b005d3' |        500 |   515.1479591836735 |         10
  'e687bdae-263f-48a6-924f-836ad67b7546' |        500 |   515.1479591836735 |         10
  'e7d0999e-e360-4036-a4e8-ae685386a1f7' |        500 |   515.1479591836735 |         10
  'e92eccb8-cca8-493e-a4af-fd01c496695a' |        500 |   515.1479591836735 |         10
  'ea5bcd39-8d4e-4283-a41d-dab54ee5e5d0' |        500 |   515.1479591836735 |         10
  'ebc3e015-6a98-4d98-981e-92792c3736eb' |        500 |   515.1479591836735 |         10
  'ed018115-50f8-4afb-b3cf-622611fac371' |        500 |   515.1479591836735 |         10
  'ee5962e2-735d-416e-a581-6440452b6958' |        500 |   515.1479591836735 |         10
  'efa7259e-2aae-4236-884b-91091e3555b3' |        500 |   515.1479591836735 |         10
  'f122e162-8519-450c-891c-8a8489d38152' |        500 |   515.1479591836735 |         10
  'f2989b24-7dca-4eba-aabf-139cd0ebc206' |        500 |   515.1479591836735 |         10
  'f3f5bf94-f7e4-4bdd-9034-93dad363d793' |        500 |   515.1479591836735 |         10
  'f5318cdb-9580-4cf6-b921-4daffd1c3d39' |        500 |   515.1479591836735 |         10
  'f66e11a3-fe1b-4d6c-ab82-ffc708a07c80' |        500 |   515.1479591836735 |         10
  'f83f5acc-58d3-4a44-9235-c662593734aa' |        500 |   515.1479591836735 |         10
  'f94d6d3d-846e-4125-8164-708fce210f6c' |        500 |   515.1479591836735 |         10
  'fa7c4387-f822-46e0-946e-6a728254fc0a' |        500 |   515.1479591836735 |         10
  'fbd6c546-9969-4f1a-afe3-2e5712051d76' |        500 |   515.1479591836735 |         10
  'fd503c07-1d30-43c9-b48c-572a301a3961' |        500 |   515.1479591836735 |         10
  'feb26b34-dc0d-4260-a116-f4d805c7b8c2' |        500 |   515.1479591836735 |         10
  'fffda92a-687d-4886-a079-4f16af3577df' |        500 |   515.1479591836735 |         10
(200 rows)

```

## Python Tool Tables

```sql
CREATE TABLE delthreads (
    id INT PRIMARY KEY,
    bval STRING,
    eval STRING
);

CREATE TABLE delruntime (
    id INT, 
    ts TIMESTAMP DEFAULT now(),
    lastval UUID,
    rowsdeleted INT,
    delpersec INT,
    PRIMARY KEY (id, ts)
);


--- misc
---
WITH delthread as (
    -- DELETE
    SELECT id
    FROM u
    WHERE id BETWEEN '00000000-0000-0000-0000-000000000000' and '014ad878-ec1c-4fe1-88d5-8caf2b362d23'
    AND delboolean is true
    LIMIT 100
)
SELECT min(id), max(id) from delthread;

WITH delthread as (
    DELETE
    FROM u
    WHERE id BETWEEN '00000000-0000-0000-0000-000000000000' and '014ad878-ec1c-4fe1-88d5-8caf2b362d23'
    AND delboolean is true
    LIMIT 10
    RETURNING (id)
)
SELECT min(id), max(id), count(*) from delthread;

WITH delthread as (
    DELETE
    FROM u
    WHERE id BETWEEN '00000000-0000-0000-0000-000000000000' and '014ad878-ec1c-4fe1-88d5-8caf2b362d23'
    AND delboolean is true
    LIMIT 10
    RETURNING (id)
)
INSERT INTO delruntime (id, lastval, rowsdeleted)
SELECT 1, max(id), count(*) from delthread
RETURNING lastval, rowsdeleted;


-- add more true booleans 
insert into u (id, delboolean) select gen_random_uuid(), true from generate_series(1,8000);

--------------------------------
-- Reporting BEFORE
--------------------------------
select delboolean, count(*)
from u
group by 1;

--------------------------------
-- Reporting AFTER
--------------------------------
select delboolean, count(*)
from u
group by 1;

select 
  min(ts) as begin_ts, 
  max(ts) as done_ts,
  extract(epoch from max(ts)-min(ts)) as runtime_sec, 
  round(sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts))) as rows_deleted_per_second
from delruntime as of system time follower_read_timestamp();


-- find oldest record
select min((crdb_internal_mvcc_timestamp/10^9)::int::timestamptz) from u;
select min((crdb_internal_mvcc_timestamp/10^9)::int::timestamptz), max((crdb_internal_mvcc_timestamp/10^9)::int::timestamptz) from u;



select count(*) from u;

   count
-----------
  1155234

select count(*) from u where (crdb_internal_mvcc_timestamp/10^9)::int::timestamptz < (now() - INTERVAL '10h');

  count
----------
  855234

select count(*) from u where (crdb_internal_mvcc_timestamp/10^9)::int::timestamptz < (now() - INTERVAL '1day');

  count
---------
  95064

WITH dstate as (
    SELECT 
        CASE WHEN (crdb_internal_mvcc_timestamp/10^9)::int::timestamptz < (now() - INTERVAL '1day') 
                THEN 'stale'
             ELSE 'current'
        END as data_state
FROM q
)
SELECT data_state, count(*)
FROM dstate
GROUP BY 1;

  data_state |  count
-------------+----------
  current    | 1045142
  stale      |   95064


select '2021-09-11'::timestamptz;

WITH dstate as (
    SELECT 
        CASE WHEN (crdb_internal_mvcc_timestamp/10^9)::int::timestamptz <  '2021-09-22 00:45:04.350686+00'::timestamptz
                THEN 'stale'
             ELSE 'current'
        END as data_state
FROM u
)
SELECT data_state, count(*)
FROM dstate
GROUP BY 1;

  data_state |  count
-------------+----------
  current    | 1255184
  stale      |  160618

```

### Scaling Batch Size

```sql

ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 00:50:00.000000' based on MVCC timestamp
---      BatchSize: 20 rows per thread
-------------------------------------------------------------------------------------------
stale : 8188746
current : 121459317

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 17:12:18.624690
           EndTimestamp : 2021-09-30 17:24:44.585776
            runtime_sec : 745.961086
rows_deleted_per_second : 10977.0
ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:00:00.000000' based on MVCC timestamp
---      BatchSize: 40 rows per thread
-------------------------------------------------------------------------------------------
stale : 9436312
current : 112023005

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 17:48:09.216993
           EndTimestamp : 2021-09-30 17:54:54.511643
            runtime_sec : 405.29465
rows_deleted_per_second : 23283.0
ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:10:00.000000' based on MVCC timestamp
---      BatchSize: 100 rows per thread
-------------------------------------------------------------------------------------------
stale : 9153034
current : 102869971

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 18:08:38.891152
           EndTimestamp : 2021-09-30 18:12:32.989158
            runtime_sec : 234.098006
rows_deleted_per_second : 39099.0
ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:20:00.000000' based on MVCC timestamp
---      BatchSize: 1000 rows per thread
-------------------------------------------------------------------------------------------
current : 93850355
stale : 9019616

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 18:19:47.445996
           EndTimestamp : 2021-09-30 18:22:04.084393
            runtime_sec : 136.638397
rows_deleted_per_second : 66011.0

ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:30:00.000000' based on MVCC timestamp
---      BatchSize: 2000 rows per thread
-------------------------------------------------------------------------------------------
stale : 8859856
current : 84990499

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 18:30:38.374008
           EndTimestamp : 2021-09-30 18:32:18.050396
            runtime_sec : 99.676388
rows_deleted_per_second : 88886.0

-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:40:00.000000' based on MVCC timestamp
---      BatchSize: 10000 rows per thread
-------------------------------------------------------------------------------------------
stale : 8698254
current : 76292245

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 18:54:01.443465
           EndTimestamp : 2021-09-30 18:56:14.269361
            runtime_sec : 132.825896
rows_deleted_per_second : 65486.0

ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast2'
---      less than '2021-09-28 01:50:00.000000' based on MVCC timestamp
---      BatchSize: 1000 rows per thread
-------------------------------------------------------------------------------------------
current : 67577919
stale : 8714326

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
         BeginTimestamp : 2021-09-30 19:07:38.403295
           EndTimestamp : 2021-09-30 19:09:38.650476
            runtime_sec : 120.247181
rows_deleted_per_second : 72470.0
```


## Trimmer Test Runs

```sql
ubuntu@ip-10-12-17-176:~$ python3 ./trimmer_by_mvcc.py
-------------------------------------------------------------------------------------------
--- DELETE stale rows in table 'bigfast6'
---      less than '2021-09-28 18:30:00.000000' based on MVCC timestamp
---      BatchSize: 400 rows per thread
---      Target Delete rowsPerSec: 20000
-------------------------------------------------------------------------------------------

199 Threads Spawned

--------------------------------------------
--- Final Report
--------------------------------------------
     BeginDeleteProcess : 2021-10-02 02:05:27.470786
       EndDeleteProcess : 2021-10-02 02:25:39.644371
            runtime_sec : 1212.173585
     total_rows_deleted : 22682739
rows_deleted_per_second : 18712.0

```

```sql

select sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts)) from delruntime 
where id=0 and 
ts between '2021-09-30 23:19:59.999999' and '2021-09-30 23:20:59.99999';

select sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts)), count(distinct id)
from delruntime
where id > -1 and
ts between '2021-09-30 23:22:59.999999' and '2021-09-30 23:25:59.99999';

select 
  sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts)) as rps, 
  count(distinct id) as active_threads
from delruntime 
where id > -1 and
ts between (now() - INTERVAL '1m10s') and (now() - INTERVAL '10s');

SELECT 
  CASE  
    WHEN sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts)) is NULL THEN 0
    ELSE sum(rowsdeleted)::float/extract(epoch from max(ts)-min(ts))
  END as rps, 
  count(distinct id) as active_threads
from delruntime 
where id > -1 and
ts between (now() - INTERVAL '1m10s') and (now() - INTERVAL '10s');
