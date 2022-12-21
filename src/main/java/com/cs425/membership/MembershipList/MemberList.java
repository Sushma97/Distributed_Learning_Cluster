package com.cs425.membership.MembershipList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Group membership list.
 * 
 * @implNote NOT thread safe during iteration. Users should implement
 * synchronization whenever the class is modified if iteration is to be used.
 */
public class MemberList implements Iterable<MemberListEntry>, Serializable {

    // TreeSet has lower insertion and deletion time
    private TreeSet<MemberListEntry> memberList;
    // self details
    private MemberListEntry owner;

    private MemberListEntry coordinator;
    private MemberListEntry backupCoordinator;

    private boolean backupPromoted = false;

    public MemberList(MemberListEntry owner) {
        assert(owner != null);

        memberList = new TreeSet<>();
        this.owner = owner;
        memberList.add(owner);
    }

    public synchronized MemberListEntry getSelfEntry() {
        return owner;
    }

    public synchronized List<MemberListEntry> getMemberList() {
        return new ArrayList<MemberListEntry>(memberList);
    }

    public synchronized int size() {
        return memberList.size();
    }

    public synchronized boolean addEntry(MemberListEntry newEntry) {
        return memberList.add(newEntry);
    }

    public synchronized void addNewOwner(MemberListEntry newOwner) {
        memberList.add(newOwner);
        assert(memberList.contains(newOwner));

        owner = newOwner;
    }

    public synchronized MemberListEntry getCoordinator() {
        return coordinator;
    }

    public synchronized void updateCoordinator(MemberListEntry newCoordinator) {
        if (memberList.contains(newCoordinator)) {
            coordinator = newCoordinator;
        }
    }

    public synchronized MemberListEntry getBackupCoordinator() {
        return backupCoordinator;
    }

    public synchronized void updateBackupCoordinator(MemberListEntry newCoordinator) {
        if (memberList.contains(newCoordinator)) {
            backupCoordinator = newCoordinator;
        }
    }

    public synchronized boolean removeEntry(MemberListEntry entry) {
        if (entry.equals(coordinator)) {
            promoteBackup();
        } else if (entry.equals(backupCoordinator)) {
            replaceBackup();
        }

        return memberList.remove(entry);
    }

    private synchronized void promoteBackup() {
        MemberListEntry temp = backupCoordinator;
        replaceBackup();
        coordinator = temp;
        backupPromoted = true;
        notifyAll();
    }

    private synchronized void replaceBackup() {
        backupCoordinator = getNewBackup();
    }

    public boolean getBackupPromoted() {
        return backupPromoted;
    }

    public synchronized boolean containsEntry(MemberListEntry entry) {
        return memberList.contains(entry);
    }

    public synchronized boolean hasSuccessor() {
        return memberList.size() > 1;
    }

    public synchronized boolean hasPredecessor() {
        return memberList.size() > 1;
    }

    /**
     * For pinging neighbors
     * @return up to n successors, if they exist
     */
    public synchronized List<MemberListEntry> getSuccessors(int n) {
        List<MemberListEntry> successors = new ArrayList<>();

        MemberListEntry successor = getSuccessor(owner);
        
        for (int i = 0; i < n && successor != null; i++) {
            successors.add(successor);
            successor = getSuccessor(successor);
        }

        assert(successors.size() <= n);
        return successors;
    }

    /**
     * Gets successor of entry such that the successor isn't the member list owner
     * @param entry the entry to find the successor of
     * @return entry's successor
     */
    private synchronized MemberListEntry getSuccessor(MemberListEntry entry) {
        MemberListEntry successor = memberList.higher(entry);
        if (successor == null) {
            successor = memberList.first();
        }
        return successor == entry || successor == owner ? null : successor;
    }

    /**
     * Gets a new backup such that the entry isn't the coordinator or backup.
     * This will be the same at every node since the list is sorted.
     * @param entry the entry to find the successor of
     * @return the first non-coordinator and non-backup entry
     */
    private synchronized MemberListEntry getNewBackup() {
        for (MemberListEntry entry : memberList) {
            if (!entry.equals(coordinator) && !entry.equals(backupCoordinator)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * @return predecessor, or null if none exist
     */
    public synchronized MemberListEntry getPredecessor() {
        MemberListEntry predecessor = memberList.lower(owner);
        if (predecessor == null) {
            predecessor = memberList.last();
        }
        return predecessor == owner ? null : predecessor;
    }

    @Override
    public synchronized String toString() {
        String stringMemberList = "Hostname\tPort\tTimestamp (Join)\n";
        stringMemberList += "_________________________";

        for (MemberListEntry entry: memberList) {
            stringMemberList += "\n";
            stringMemberList += entry.toString();

            if (entry.equals(owner)) {
                stringMemberList += "\t(Self)";
            }
            if (entry.equals(coordinator)) {
                stringMemberList += "\t(Coordinator)";
            }
            if (entry.equals(backupCoordinator)) {
                stringMemberList += "\t(Backup Coordinator)";
            }
        }

        return stringMemberList;
    }

    @Override
    public synchronized Iterator<MemberListEntry> iterator() {
        return memberList.iterator();
    }
    
}
