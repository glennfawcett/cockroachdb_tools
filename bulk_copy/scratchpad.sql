CREATE TABLE delivery_min_earning_info_tmp_1 (
  delivery_id INT8 NOT NULL,
  shift_id INT8 NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp():::TIMESTAMPTZ,
  distinct_active_time INT8 NOT NULL,
  minimum_wage_amount INT8 NOT NULL,
  mileage_reimbursement_amount INT8 NOT NULL,
  pickup_zip_code STRING NOT NULL,
  pickup_zip_code_pay_target_uuid UUID NOT NULL,
  CONSTRAINT pk_delivery_min_earning_info PRIMARY KEY (shift_id DESC, delivery_id DESC, created_at DESC)
);

INSERT INTO delivery_min_earning_info_tmp_1
SELECT i*43, i, now(), i, i+1, i+2, 'ZIP', gen_random_uuid()
FROM generate_series(1,18000) as i;

INSERT INTO delivery_min_earning_info_tmp_1
SELECT i*43, i*-1000, now(), i*-1, i+1, i+2, 'ZIP', gen_random_uuid()
FROM generate_series(1,10099) as i;