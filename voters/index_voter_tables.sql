BEGIN;
CREATE INDEX "voters_voter_fname" ON "voters_voter" ("fname");
CREATE INDEX "voters_voter_lname" ON "voters_voter" ("lname");
CREATE INDEX "voters_voter_birthdate" ON "voters_voter" ("birthdate");
CREATE INDEX "voters_voter_county_slug" ON "voters_voter" ("county_slug");
CREATE INDEX "voters_voter_addr1" ON "voters_voter" ("addr1");
CREATE INDEX "voters_voter_voter_id" ON "voters_voter" ("voter_id");
COMMIT;
