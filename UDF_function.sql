create function test_udf2 (num_886 varchar,
                          reachable_0_365 varchar,
                          has_social_activity varchar,
                          has_network_0_180 varchar
                          )
  returns float
stable
as $$
  if (num_886.startswith('886') and num_886.startswith('8869')
    and (reachable_0_365 == '0' or reachable_0_365 == '' or reachable_0_365 is None)
    and (has_social_activity == '0' or has_social_activity == '' or has_social_activity is None)
    and (has_network_0_180 == '')):
    return 98
  elif (num_886.startswith('886') and num_886.startswith('8869')
    and (reachable_0_365 == '0' or reachable_0_365 == '' or reachable_0_365 is None)
    and (has_social_activity == '1')
    and (has_network_0_180 == '')):
    return 90
$$ language plpythonu;


