package org.apache.bookkeeper.bookie;

public class InternalReadEntryAux {

    private Long ledgerID;
    private Long entryID;
    private Long location;
    private Boolean validateEntry;
    private Boolean expected;

    public InternalReadEntryAux(Long ledgerID, Long entryID, Long location, Boolean validateEntry, Boolean expected) {
        this.ledgerID = ledgerID;
        this.entryID = entryID;
        this.location = location;
        this.validateEntry = validateEntry;
        this.expected = expected;
    }

    public Long getLedgerID() {
        return ledgerID;
    }

    public void setLedgerID(Long ledgerID) {
        this.ledgerID = ledgerID;
    }

    public Long getEntryID() {
        return entryID;
    }

    public void setEntryID(Long entryID) {
        this.entryID = entryID;
    }

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    public Boolean getValidateEntry() {
        return validateEntry;
    }

    public void setValidateEntry(Boolean validateEntry) {
        this.validateEntry = validateEntry;
    }

    public Boolean getExpected() {
        return expected;
    }

    public void setExpected(Boolean expected) {
        this.expected = expected;
    }
}