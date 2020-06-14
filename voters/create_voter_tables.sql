BEGIN;
CREATE TABLE "voters_voter" ("id" serial NOT NULL PRIMARY KEY, "lname" varchar(255) NOT NULL, "fname" varchar(255) NOT NULL, "mname" varchar(255) NOT NULL, "suffix" varchar(255) NOT NULL, "addr1" varchar(255) NOT NULL, "addr2" varchar(255) NOT NULL, "city" varchar(255) NOT NULL, "zip" varchar(255) NOT NULL, "gender" varchar(255) NOT NULL, "race" varchar(255) NOT NULL, "birthdate" varchar(255) NOT NULL, "party" varchar(255) NOT NULL, "areacode" varchar(255) NOT NULL, "phone" varchar(255) NOT NULL, "email" varchar(255) NOT NULL, "voter_id" varchar(255) NOT NULL, "county_slug" varchar(255) NOT NULL, "source_date" date NOT NULL);
ALTER TABLE "voters_voter" ALTER COLUMN "addr1" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "addr2" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "areacode" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "birthdate" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "city" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "county_slug" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "email" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "fname" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "gender" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "lname" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "mname" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "party" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "phone" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "race" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "suffix" DROP NOT NULL;
ALTER TABLE "voters_voter" ALTER COLUMN "zip" DROP NOT NULL;
COMMIT;
