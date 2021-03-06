-- Creates table containing referrer, referred pairs and fraction of internal views
INSERT OVERWRITE DIRECTORY '/user/hive/referral_fractions'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT REFERRALS.REFERRER, 
		REFERRALS.REFERRED, 
		INTERNAL_FRACTION.INTERNAL_VIEWS, 
		INTERNAL_FRACTION.TOTAL_VIEWS, 
		INTERNAL_FRACTION.INTERNAL_FRACTION
FROM REFERRALS JOIN INTERNAL_FRACTION
ON (REFERRALS.REFERRED = INTERNAL_FRACTION.PAGE)
ORDER BY REFERRALS.REFERRER ASC;

-- Queries following series of articles with highest fraction of internal views
SELECT *
FROM REFERRAL_FRACTIONS
WHERE REFERRER='Hotel_California'
ORDER BY INTERNAL_FRACTION DESC;

SELECT *
FROM REFERRAL_FRACTIONS
WHERE REFERRER='Eagles_(band)'
ORDER BY INTERNAL_FRACTION DESC
LIMIT 10;

SELECT *
FROM REFERRAL_FRACTIONS
WHERE REFERRER='Emerson,_Lake_&_Palmer' AND INTERNAL_FRACTION < 1
ORDER BY INTERNAL_FRACTION DESC
LIMIT 10;

SELECT *
FROM REFERRAL_FRACTIONS
WHERE REFERRER='Atomic_Rooster' AND INTERNAL_FRACTION < 1
ORDER BY INTERNAL_FRACTION DESC
LIMIT 10;